"""数据采样领域服务：管理流式数据的采样策略。

增强能力: 采样策略应用、样本特征分析、分层采样、样本保留校验、输出到debug_trace

序号17 修复说明：
  * 分层采样改为“先保底每层一份，再按比例分配剩余配额”，尾部分层不会被整体截断；
  * 截断损失、被截断分层、是否扩容上限全部写入样本报告（_sample_report）；
  * 同一事件按 event_id 去重，重复采样不会重复累计事件；
  * 并发/重复调用同一策略时按幂等键（策略 + 数据指纹 + 参数）复用已有批次，
    只生效一次；批次写入是原子的，异常时回滚，不留下半份分层样本数据。
"""
from __future__ import annotations

import copy
import threading
import time
from typing import Any

from app.sampling import (
    ISSUE_ID,
    dedupe_and_group,
    event_identity,
    random_sample,
    sampled_stratum_counts,
    stratified_sample,
)
from app.services.base import BaseModuleService
from app.store import store

_SAMPLING_LOCK = threading.RLock()

# 序号17 修复台账：记录每次生效的修复版本、幂等结果与回滚基线，
# 供其他模块同步核对（采样与调试消费同一份台账）。
_FIX_LEDGER: list[dict[str, Any]] = []
_LEDGER_LIMIT = 200


def _record_ledger(entry: dict[str, Any]) -> None:
    _FIX_LEDGER.append(entry)
    del _FIX_LEDGER[:-_LEDGER_LIMIT]


class DataSamplerService(BaseModuleService):
    MODULE = "data_sampler"
    ENTITY = "采样策略"
    LIST_FIELDS = ["策略名称", "采样方式", "采样比例", "分组键", "保留特征", "样本上限", "采样窗口"]
    REQUIRED_FIELDS = ["策略名称", "采样方式", "采样比例"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用策略": "草稿", "停用策略": "已启用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    def apply_sampling(
        self, sampler_name: str, data: list[dict[str, Any]],
        *, idempotency_key: str | None = None,
        dry_run: bool = False,
        method: str | None = None,
        group_key: str | None = None,
        ratio: float | None = None,
        max_samples: int | None = None,
        cap_policy: str = "expand",
    ) -> dict[str, Any]:
        """对数据执行采样策略：随机采样或分层采样。

        返回值里同时包含样本报告摘要；完整报告写入结果记录 `_sample_report`。
        method/group_key/ratio/max_samples 只覆盖本次调用，不修改策略配置。
        """
        sampler = self._find_sampler(sampler_name)
        if sampler is None:
            return {"ok": False, "error": f"采样策略「{sampler_name}」不存在"}

        method = str(method or sampler.get("采样方式", "random") or "random")
        ratio = self._float(ratio if ratio is not None else sampler.get("采样比例", "0.1"), 0.1)
        max_samples = self._int(
            max_samples if max_samples is not None else sampler.get("样本上限", "100"), 100,
        )
        group_key = str(
            group_key if group_key is not None else sampler.get("分组键", "") or ""
        )
        features = [f.strip() for f in str(sampler.get("保留特征", "")).split(",") if f.strip()]

        # 幂等键：显式传入优先，其次按策略+数据指纹+参数自动生成
        unique, duplicates, strata = dedupe_and_group(data, group_key if method == "stratified" else None)
        fingerprint = self._fingerprint(
            unique, method, ratio, max_samples, group_key, cap_policy,
        )
        key = idempotency_key or fingerprint
        seed_material = f"{sampler_name}|{key}"

        with _SAMPLING_LOCK:
            existing = self.find_batch(key)
            if existing is not None:
                _record_ledger({
                    "issue": ISSUE_ID,
                    "batch_id": existing.get("id"),
                    "result_version": (existing.get("_sample_report") or {}).get("result_version"),
                    "cap_policy": (existing.get("_sample_report") or {}).get("cap_policy"),
                    "idempotent": True,
                    "idempotency_key": key,
                    "applied_at": time.time(),
                })
                return self._summary(
                    existing, deduplicated=True, duplicate_events=duplicates,
                )

            if method == "stratified":
                sampled, report = stratified_sample(
                    data, group_key or "__default__", ratio, max_samples,
                    seed_material, cap_policy,
                )
            else:
                sampled, report = random_sample(data, ratio, max_samples, seed_material)
            report["sampler"] = sampler_name
            report["idempotency_key"] = key
            report["dedupe_dropped"] = duplicates
            report["generated_at"] = time.time()

            if dry_run:
                return {
                    "ok": True,
                    "dry_run": True,
                    "sampler": sampler_name,
                    "method": method,
                    "total": report["total_events"],
                    "sampled": report["sampled_total"],
                    "ratio": round(report["sampled_total"] / max(report["total_events"], 1), 4),
                    "report": report,
                }

            entry = self._build_batch_entry(
                sampler_name, sampler, method, ratio, max_samples, group_key,
                features, sampled, report, strata if method == "stratified" else {},
                duplicates,
            )
            rows = store.rows(self.MODULE)
            snapshot = list(rows)
            try:
                rows.append(entry)
            except Exception:
                # 事务回滚：恢复写入前的行集合，避免残留半个批次
                rows[:] = snapshot
                raise
            # 批次本身就是采样与调试共用的产物：调试会话按 batch_id 引用同一份
            # 分层样本与样本报告，不另外复制一份，回滚时也不会留下孤儿会话。
            _record_ledger({
                "issue": ISSUE_ID,
                "batch_id": entry.get("id"),
                "result_version": report.get("result_version"),
                "cap_policy": report.get("cap_policy"),
                "strata_total": report.get("strata_total"),
                "strata_covered": report.get("strata_covered"),
                "truncated_total": report.get("truncated_total"),
                "duplicates_dropped": report.get("duplicates_dropped"),
                "idempotent": False,
                "idempotency_key": key,
                "rollback_baseline": len(snapshot) - 1,
                "applied_at": report.get("generated_at"),
            })
            result = self._summary(entry, deduplicated=False, duplicate_events=duplicates)
            result["report"] = report
            return result

    # ---- 查询与复核 ----

    def fix_ledger(self, limit: int = 50) -> list[dict[str, Any]]:
        """序号17 修复台账，其他模块可据此同步更新。"""
        return list(reversed(_FIX_LEDGER[-max(1, limit):]))

    def read_events_from_source(
        self, source_name: str, batch_size: int = 20,
    ) -> dict[str, Any]:
        """从流式源端读取一批事件作为采样输入。

        源端存在但状态为异常断连/已下线时拒绝读取，避免用不完整数据出样本报告。
        """
        source = next(
            (r for r in store.rows("stream_source") if r.get("源端名称") == source_name),
            None,
        )
        if source is not None and source.get("status") in ("异常断连", "已下线"):
            return {
                "ok": False,
                "error": f"源端「{source_name}」状态为{source.get('status')}，无法读取采样事件",
            }
        from app.pipeline import simulate_stream_batch

        events = simulate_stream_batch(source_name, batch_size)
        return {
            "ok": True,
            "source": source_name,
            "found": source is not None,
            "events": events,
        }

    def _visible_rows(self) -> list[dict[str, Any]]:
        """策略列表只展示策略本身；采样批次是运行期产物，去复核面板里看。"""
        return [
            row for row in store.rows(self.MODULE)
            if not row.get("_sample_report") and not row.get("_batch_key")
        ]

    def find_batch(self, key: str) -> dict[str, Any] | None:
        """按幂等键查找已经生效的采样批次（后写入的覆盖先写入的）。"""
        for row in reversed(store.rows(self.MODULE)):
            if row.get("_batch_key") == key and row.get("_sample_report"):
                return row
        return None

    def get_sample_report(self, batch_id: int | None = None) -> dict[str, Any] | None:
        """取样本报告；不传批次号时取该策略最近一次批次。"""
        if batch_id is not None:
            row = store.find(self.MODULE, batch_id)
        else:
            row = self.latest_batch()
        if row is None or not row.get("_sample_report"):
            return None
        report = copy.deepcopy(row["_sample_report"])
        report["batch_id"] = row.get("id")
        report["sampled"] = row.get("_sample_count", 0)
        return report

    def latest_batch(self) -> dict[str, Any] | None:
        for row in reversed(store.rows(self.MODULE)):
            if row.get("_sample_report") and row.get("_batch_key"):
                return row
        return None

    def list_batches(self, limit: int = 20) -> list[dict[str, Any]]:
        """最近采样批次摘要，供页面下拉选择。"""
        batches = [
            r for r in store.rows(self.MODULE) if r.get("_sample_report")
        ]
        batches.sort(key=lambda r: int(r.get("id", 0)), reverse=True)
        return [
            {
                "batch_id": row.get("id"),
                "sampler": row.get("策略名称", ""),
                "method": (row.get("_sample_report") or {}).get("method", ""),
                "group_key": row.get("分组键", ""),
                "sampled": row.get("_sample_count", 0),
                "strata_total": (row.get("_sample_report") or {}).get("strata_total", 0),
                "strata_covered": (row.get("_sample_report") or {}).get("strata_covered", 0),
                "truncated_total": (row.get("_sample_report") or {}).get("truncated_total", 0),
                "sampled_at": row.get("_sampled_at"),
            }
            for row in batches[:limit]
        ]

    def list_stratum_counts(self, batch_id: int | None = None) -> list[dict[str, Any]]:
        """列出每个分层入选数量，供采样页面直接展示。"""
        row = store.find(self.MODULE, batch_id) if batch_id is not None else self.latest_batch()
        if row is None or not row.get("_sample_report"):
            return []
        return [
            {
                "stratum": item["stratum"],
                "available": item["available"],
                "selected": item["selected"],
                "loss": item["loss"],
                "truncated": item["truncated"],
                "covered": item["covered"],
            }
            for item in row["_sample_report"].get("strata", [])
        ]

    def verify_sample_coverage(
        self,
        report: dict[str, Any],
        sampled: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """核对分层样本数据与样本报告是否一致，返回缺失分层列表。"""
        strata = report.get("strata", []) if report else []
        group_key = report.get("group_key", "") if report else ""
        report_missing = [
            item["stratum"] for item in strata
            if item.get("available", 0) > 0 and item.get("selected", 0) <= 0
        ]
        actual_missing: list[str] = []
        mismatched: list[str] = []
        if sampled is not None and group_key:
            counts = sampled_stratum_counts(sampled, group_key)
            for item in strata:
                stratum = item["stratum"]
                if counts.get(stratum, 0) <= 0:
                    actual_missing.append(stratum)
                elif counts.get(stratum, 0) != item.get("selected"):
                    mismatched.append(stratum)
            reported = {item["stratum"] for item in strata}
            actual_extra = sorted(set(counts) - reported)
        else:
            actual_extra = []
        missing = sorted(set(report_missing) | set(actual_missing))
        return {
            "ok": not missing and not mismatched and not actual_extra,
            "issue": ISSUE_ID,
            "missing_strata": missing,
            "count_mismatch": sorted(mismatched),
            "unexpected_strata": actual_extra,
            "strata_total": len(strata),
            "strata_covered": sum(1 for item in strata if item.get("selected", 0) > 0),
            "coverage_complete": not missing,
        }

    # ---- 已有能力 ----

    def compute_sample_features(
        self, sampler_name: str, sampled_data: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """分析采样数据的统计特征：分布、均值、方差等。"""
        if not sampled_data:
            return {"ok": True, "sampler": sampler_name, "features": {}}

        values = []
        for item in sampled_data:
            payload = item.get("payload", item)
            val = payload.get("value", 0)
            if not isinstance(val, (int, float)):
                try:
                    val = float(val)
                except (ValueError, TypeError):
                    val = 0.0
            values.append(val)

        n = len(values)
        mean = sum(values) / n
        variance = sum((v - mean) ** 2 for v in values) / n
        stddev = variance ** 0.5
        sorted_vals = sorted(values)

        features = {
            "count": n,
            "mean": round(mean, 4),
            "stddev": round(stddev, 4),
            "min": sorted_vals[0] if sorted_vals else 0,
            "max": sorted_vals[-1] if sorted_vals else 0,
            "p50": sorted_vals[n // 2] if n > 0 else 0,
            "p95": sorted_vals[int(n * 0.95)] if n > 1 else (sorted_vals[0] if n > 0 else 0),
        }
        return {"ok": True, "sampler": sampler_name, "features": features}

    def list_threshold_breaches(self, sampler_name: str) -> list[dict[str, Any]]:
        """列出超出样本上限的采样批次；修复后扩容上限会单独标注。"""
        breaches = []
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == sampler_name and r.get("_sample_count"):
                report = r.get("_sample_report") or {}
                limit = report.get("effective_limit", self._int(r.get("样本上限", "100"), 100))
                if r["_sample_count"] > limit:
                    breaches.append({
                        "sampler": r.get("策略名称", ""),
                        "sample_count": r["_sample_count"],
                        "limit": limit,
                        "exceed_by": r["_sample_count"] - limit,
                    })
        return breaches

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        batches = [r for r in rows if r.get("_sample_report")]
        base["total_samples"] = sum(
            r.get("_sample_count", 0) for r in batches if r.get("_sample_count")
        )
        base["sample_batches"] = len(batches)
        base["truncated_batches"] = sum(
            1 for r in batches if (r.get("_sample_report") or {}).get("truncated_strata")
        )
        return base

    # ---- 内部工具 ----

    def _find_sampler(self, name: str) -> dict[str, Any] | None:
        return next(
            (r for r in store.rows(self.MODULE) if r.get("策略名称") == name and not r.get("_sample_report")),
            None,
        )

    @staticmethod
    def _int(value: Any, default: int) -> int:
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _float(value: Any, default: float) -> float:
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _fingerprint(
        unique: list[dict[str, Any]], method: str, ratio: float,
        max_samples: int, group_key: str, cap_policy: str = "expand",
    ) -> str:
        import hashlib

        material = "|".join(sorted(event_identity(item) for item in unique))
        digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]
        return f"sample:{method}:{group_key}:{ratio}:{max_samples}:{cap_policy}:{digest}"


    def _build_batch_entry(
        self,
        sampler_name: str,
        sampler: dict[str, Any],
        method: str,
        ratio: float,
        max_samples: int,
        group_key: str,
        features: list[str],
        sampled: list[dict[str, Any]],
        report: dict[str, Any],
        strata: dict[str, list[dict[str, Any]]],
        duplicates: int,
    ) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        entry = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "策略名称": sampler_name,
            "采样方式": method,
            "采样比例": str(ratio),
            "分组键": group_key,
            "保留特征": ",".join(features),
            "样本上限": str(max_samples),
            "采样窗口": sampler.get("采样窗口", "60"),
            "status": "已启用",
            "pending": False,
            "abnormal": bool(report.get("uncovered_strata")),
            "_batch_key": report["idempotency_key"],
            "_sampled_data": sampled,
            "_sample_count": len(sampled),
            "_total_count": report["total_events"],
            "_duplicate_count": duplicates,
            "_sample_report": report,
            "_strata": {key: len(items) for key, items in strata.items()},
            "_sampled_at": time.time(),
        }
        return entry

    @staticmethod
    def _summary(
        row: dict[str, Any], *, deduplicated: bool, duplicate_events: int,
    ) -> dict[str, Any]:
        report = row.get("_sample_report") or {}
        return {
            "ok": True,
            "sampler": row.get("策略名称", ""),
            "method": report.get("method", row.get("采样方式", "")),
            "batch_id": row.get("id"),
            "idempotency_key": row.get("_batch_key"),
            "deduplicated": deduplicated,
            "total": report.get("total_events", row.get("_total_count", 0)),
            "sampled": row.get("_sample_count", 0),
            "ratio": round(
                row.get("_sample_count", 0) / max(report.get("total_events", 0), 1), 4
            ),
            "duplicates_dropped": report.get("duplicates_dropped", 0),
            "request_duplicates": duplicate_events,
            "strata_total": report.get("strata_total", 0),
            "strata_covered": report.get("strata_covered", 0),
            "truncated_strata": report.get("truncated_strata", []),
            "truncated_total": report.get("truncated_total", 0),
            "cap_expanded": report.get("cap_expanded", False),
            "cap_policy": report.get("cap_policy", "none"),
            "limit_overrun": report.get("limit_overrun", 0),
            "result_version": report.get("result_version", ""),
            "coverage_complete": report.get("coverage_complete", True),
            "report": {
                "sampled_total": report.get("sampled_total", row.get("_sample_count", 0)),
                "strata": report.get("strata", []),
            },
        }
