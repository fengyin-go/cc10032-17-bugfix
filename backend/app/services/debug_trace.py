"""调试追踪领域服务：针对特定算子启动调试会话。

增强能力: 异常标记分析、中间结果检查、会话持续时间统计、关联采样数据分析

序号17 修复说明：
  * 会话关联采样批次后，生成异常结论前先核对分层样本数据与样本报告；
  * 任一分层缺失（有事件但没进样本、或报告与样本对不上）时阻止生成异常结论，
    不再把“未抽样的分层”当成正常；
  * 报告里的异常率只按已覆盖分层统计，并显式列出被截断/未覆盖的分层。
"""
from __future__ import annotations

import time
from typing import Any

from app.sampling import ISSUE_ID
from app.services.base import BaseModuleService
from app.services.data_sampler import DataSamplerService
from app.store import store


class DebugTraceService(BaseModuleService):
    MODULE = "debug_trace"
    ENTITY = "调试会话"
    LIST_FIELDS = ["会话名称", "目标算子", "采样数据", "中间结果", "异常标记", "开始时间", "结束时间"]
    REQUIRED_FIELDS = ["会话名称", "目标算子", "采样数据"]
    STATUS_ORDER = ["运行中", "已完成", "已终止"]
    ACTION_RULES = {"终止会话": "运行中"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        self._sampler = DataSamplerService()

    # ---- 关联采样批次 ----

    def attach_sampling_batch(
        self, session_name: str, batch_id: int | None = None,
    ) -> dict[str, Any]:
        """把调试会话绑定到某个采样批次；不传批次号时取最近一次有效批次。"""
        session = self._find_session(session_name)
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}
        row = (
            store.find("data_sampler", batch_id)
            if batch_id is not None
            else self._sampler.latest_batch()
        )
        if row is None or not row.get("_sample_report"):
            return {"ok": False, "error": "没有可用的采样批次，无法绑定调试会话"}
        sampled_ids = [
            sample_id
            for item in row["_sample_report"].get("strata", [])
            for sample_id in item.get("selected_ids", [])
        ]
        session["_sample_batch_id"] = row.get("id")
        session["采样数据"] = ",".join(sampled_ids[:3]) if sampled_ids else ""
        session["中间结果"] = str(row.get("_sample_count", 0))
        session["_coverage_blocked"] = False
        return {
            "ok": True,
            "session": session_name,
            "batch_id": row.get("id"),
            "sampled": row.get("_sample_count", 0),
        }

    def _resolve_batch(self, session: dict[str, Any]) -> dict[str, Any] | None:
        batch_id = session.get("_sample_batch_id")
        if batch_id is not None:
            row = store.find("data_sampler", int(batch_id))
            if row is not None and row.get("_sample_report"):
                return row
        return None

    def check_sample_coverage(self, session_name: str) -> dict[str, Any]:
        """核对会话关联的采样批次：分层样本数据与样本报告必须一致且无缺失层。"""
        session = self._find_session(session_name)
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}
        row = self._resolve_batch(session)
        if row is None:
            return {
                "ok": False,
                "issue": ISSUE_ID,
                "reason": "no_sample_batch",
                "message": "调试会话没有关联采样批次，无法确认分层覆盖，禁止生成异常结论",
                "missing_strata": [],
                "truncated_strata": [],
            }
        report = row["_sample_report"]
        verification = self._sampler.verify_sample_coverage(report, row.get("_sampled_data") or [])
        verification["batch_id"] = row.get("id")
        verification["sampler"] = row.get("策略名称")
        verification["truncated_strata"] = report.get("truncated_strata", [])
        verification["strata"] = [
            {
                "stratum": item["stratum"],
                "available": item["available"],
                "selected": item["selected"],
                "loss": item["loss"],
                "truncated": item["truncated"],
                "covered": item["covered"],
            }
            for item in report.get("strata", [])
        ]
        if verification["missing_strata"]:
            verification["message"] = (
                "以下分层有事件但没有可追踪样本："
                + "、".join(verification["missing_strata"])
                + "，已阻止生成异常结论"
            )
        else:
            verification["message"] = "分层样本覆盖完整"
        return verification

    # ---- 异常分析 ----

    def analyze_anomaly_patterns(
        self, session_name: str,
    ) -> dict[str, Any]:
        """分析调试会话中的异常标记模式；覆盖不完整时拒绝给出异常结论。"""
        session = self._find_session(session_name)
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}

        coverage = self.check_sample_coverage(session_name)
        if not coverage.get("ok"):
            return {
                "ok": False,
                "issue": ISSUE_ID,
                "session": session_name,
                "blocked": True,
                "coverage": coverage,
                "error": coverage.get("message") or "分层样本覆盖不完整，已阻止生成异常结论",
            }

        anomaly_mark = session.get("异常标记", "none")
        intermediate = session.get("中间结果", "")
        sampled = session.get("采样数据", "")

        patterns = {
            "anomaly": anomaly_mark,
            "intermediate_count": intermediate,
            "sampled_data_ref": sampled,
            "duration": 0,
            "strata_covered": coverage.get("strata_covered", 0),
            "strata_total": coverage.get("strata_total", 0),
            "truncated_strata": coverage.get("truncated_strata", []),
        }

        try:
            start = session.get("开始时间", "")
            end = session.get("结束时间", "")
            if start and end:
                patterns["duration"] = f"{start} → {end}"
        except Exception:
            pass

        target_operator = session.get("目标算子", "")
        linked_states = [
            s for s in store.rows("state_store")
            if target_operator in str(s.get("_operator", ""))
        ]
        patterns["linked_state_entries"] = len(linked_states)

        return {
            "ok": True,
            "session": session_name,
            "target": target_operator,
            "patterns": patterns,
            "coverage": coverage,
            "status": session.get("status", ""),
        }

    def complete_session_with_analysis(
        self, session_name: str,
    ) -> dict[str, Any]:
        """完成调试会话并生成分析报告；覆盖有缺失时阻止结论并保留会话原状态。"""
        session = self._find_session(session_name)
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}

        if session.get("status") not in ("运行中",):
            return {"ok": False, "error": f"会话状态为「{session.get('status')}」, 不可完成"}

        coverage = self.check_sample_coverage(session_name)
        if not coverage.get("ok"):
            session["_coverage_blocked"] = True
            return {
                "ok": False,
                "issue": ISSUE_ID,
                "session": session_name,
                "blocked": True,
                "coverage": coverage,
                "error": coverage.get("message") or "分层样本覆盖不完整，已阻止生成异常结论",
            }

        end_time = time.strftime("%Y-%m-%d %H:%M:%S")

        sampled = session.get("采样数据", "")
        intermediate = session.get("中间结果", "")
        anomaly = "none"

        if not sampled:
            anomaly = "no_sampled_data"
        elif not intermediate or intermediate == "0":
            anomaly = "no_intermediate_results"

        # 截断信息不进“异常标记”（异常标记只描述数据本身），但必须随结论一起展示，
        # 避免读者把“只抽到 1 条的分层”当成完整覆盖。
        truncated = coverage.get("truncated_strata", [])
        session["结束时间"] = end_time
        session["异常标记"] = anomaly
        session["status"] = "已完成"
        session["pending"] = False
        session["_coverage_blocked"] = False
        session["_coverage_note"] = (
            f"以下分层样本已按上限截断：{'、'.join(truncated)}" if truncated else "分层样本完整"
        )

        analysis = {
            "target": session.get("目标算子", ""),
            "sampled_data_present": bool(sampled),
            "intermediate_count": intermediate,
            "anomaly_detected": anomaly != "none",
            "anomaly_type": anomaly,
            "strata_covered": coverage.get("strata_covered", 0),
            "strata_total": coverage.get("strata_total", 0),
            "truncated_strata": truncated,
            "truncated_loss": sum(
                item.get("loss", 0) for item in coverage.get("strata", [])
            ),
            "coverage_complete": coverage.get("ok", False),
            "coverage_note": session["_coverage_note"],
            "completed_at": end_time,
        }

        return {"ok": True, "session": session_name, "analysis": analysis}

    # ---- 已有能力 ----

    def list_session_statistics(self) -> dict[str, Any]:
        """列出所有调试会话的统计信息。"""
        sessions = [
            r for r in store.rows(self.MODULE)
            if r.get("id", 0) < 1000
        ]
        running = sum(1 for s in sessions if s.get("status") == "运行中")
        completed = sum(1 for s in sessions if s.get("status") == "已完成")
        terminated = sum(1 for s in sessions if s.get("status") == "已终止")
        anomalous = sum(1 for s in sessions if s.get("异常标记") != "none")

        return {
            "total": len(sessions),
            "running": running,
            "completed": completed,
            "terminated": terminated,
            "with_anomalies": anomalous,
            "anomaly_rate": anomalous / max(len(sessions), 1),
        }

    def link_to_sampler(self, session_name: str) -> dict[str, Any]:
        """将调试会话关联到采样策略。"""
        session = self._find_session(session_name)
        if session is None:
            return {"ok": False, "error": f"调试会话「{session_name}」不存在"}

        target = session.get("目标算子", "")
        batch_id = session.get("_sample_batch_id")
        linked = []
        for r in store.rows("data_sampler"):
            if not r.get("_sample_count"):
                continue
            is_bound = batch_id is not None and r.get("id") == batch_id
            if not is_bound and target not in str(r.get("策略名称", "")):
                continue
            report = r.get("_sample_report") or {}
            linked.append({
                "sampler": r.get("策略名称", ""),
                "batch_id": r.get("id"),
                "sample_count": r.get("_sample_count", 0),
                "strata_covered": report.get("strata_covered", 0),
                "strata_total": report.get("strata_total", 0),
                "truncated_strata": report.get("truncated_strata", []),
            })
        session["_linked_samplers"] = linked

        return {
            "ok": True,
            "session": session_name,
            "linked_samplers": len(linked),
            "details": linked,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["running_sessions"] = sum(1 for r in rows if r.get("status") == "运行中")
        base["completed_sessions"] = sum(1 for r in rows if r.get("status") == "已完成")
        blocked = [r for r in rows if r.get("_coverage_blocked")]
        base["coverage_blocked_sessions"] = len(blocked)
        return base

    # ---- 内部工具 ----

    def _find_session(self, name: str) -> dict[str, Any] | None:
        return next(
            (r for r in store.rows(self.MODULE) if r.get("会话名称") == name),
            None,
        )
