"""序号17 采样修复的纯逻辑层：分层、配额分配、截断台账。

这里不依赖 store / FastAPI，方便被数据采样、调试追踪和测试共用。

修复要点（对应序号17）：
  * 每个非空分层在样本上限内至少保留 1 条可追踪样本；
  * 分层配额先保底再按比例分配，尾部分层不会被整体截断清空；
  * 上限不足以覆盖全部非空分层时自动扩容到分层数（每层 1 条），并显式记录；
  * 被截断的分层、截断损失全部写入样本报告；
  * 同一事件（同 event_id / 同内容指纹）只计一次，重复采样不会重复累计；
  * 采样顺序稳定（种子由配置 + 数据指纹决定），同一输入重复执行结果一致。
"""
from __future__ import annotations

import hashlib
import json
import random
from collections import Counter
from typing import Any

ISSUE_ID = "17"
DEFAULT_STRATUM = "__default__"
# 两种修复策略各自产出不同的结果版本，便于在序号17下按版本复核
RESULT_VERSION = {
    "expand": "issue17.expand.v1",
    "compress": "issue17.compress.v1",
    "none": "issue17.random.v1",
}


# ---------------------------------------------------------------------------
# 基础工具
# ---------------------------------------------------------------------------

def stratum_of(item: dict[str, Any], group_key: str | None) -> str:
    """取事件所属分层；分组键既支持 payload 内字段，也支持平铺字段。"""
    if not group_key:
        return DEFAULT_STRATUM
    payload = item.get("payload")
    if isinstance(payload, dict) and group_key in payload:
        value = payload.get(group_key)
    else:
        value = item.get(group_key, DEFAULT_STRATUM)
    if value is None or value == "":
        return DEFAULT_STRATUM
    return str(value)


def event_identity(item: dict[str, Any]) -> str:
    """事件唯一标识：优先业务 id，缺失时用内容指纹，保证可追踪、可去重。"""
    for key in ("event_id", "offset", "record_id", "id"):
        value = item.get(key)
        if value not in (None, ""):
            return f"{key}:{value}"
    fingerprint = hashlib.sha256(
        json.dumps(
            {
                "payload": item.get("payload"),
                "event_time": item.get("event_time"),
                "topic": item.get("topic"),
                "source": item.get("source"),
            },
            sort_keys=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()[:20]
    return f"fp:{fingerprint}"


def stable_seed(*parts: Any) -> int:
    material = "|".join(str(part) for part in parts)
    return int(hashlib.sha256(material.encode("utf-8")).hexdigest()[:16], 16)


def dedupe_and_group(
    events: list[dict[str, Any]], group_key: str | None,
) -> tuple[list[dict[str, Any]], int, dict[str, list[dict[str, Any]]]]:
    """按事件标识全局去重后分层。

    返回 (唯一事件列表, 丢弃的重复条数, 按首次出现顺序排列的分层字典)。
    同一 event_id 即使出现在不同分层里也只计一次，避免重复累计。
    """
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    duplicates = 0
    strata: dict[str, list[dict[str, Any]]] = {}
    for item in events or []:
        identity = event_identity(item)
        if identity in seen:
            duplicates += 1
            continue
        seen.add(identity)
        unique.append(item)
        strata.setdefault(stratum_of(item, group_key), []).append(item)
    return unique, duplicates, strata


# ---------------------------------------------------------------------------
# 配额分配
# ---------------------------------------------------------------------------

def _distribute_remaining(
    allocation: dict[str, int],
    sizes: dict[str, int],
    remaining: int,
    order: list[str],
) -> None:
    """把剩余配额按最大余数法分给各层，且不超过该层可用事件数。"""
    while remaining > 0:
        eligible = [key for key in order if allocation[key] < sizes[key]]
        if not eligible:
            return
        total_weight = sum(sizes[key] for key in eligible)
        if total_weight <= 0:
            return
        quotas = {key: remaining * sizes[key] / total_weight for key in eligible}
        assigned = 0
        for key in eligible:
            extra = min(int(quotas[key]), sizes[key] - allocation[key])
            if extra > 0:
                allocation[key] += extra
                assigned += extra
        remaining -= assigned
        if remaining <= 0:
            return
        # 余数从大到小补，层顺序作为稳定 tie-breaker
        order_index = {key: index for index, key in enumerate(order)}
        ranked = sorted(
            eligible,
            key=lambda key: (-(quotas[key] - int(quotas[key])), order_index[key]),
        )
        progressed = False
        for key in ranked:
            if remaining <= 0:
                break
            if allocation[key] < sizes[key]:
                allocation[key] += 1
                remaining -= 1
                progressed = True
        if not progressed:
            return


def plan_allocation(
    sizes: dict[str, int],
    ratio: float,
    requested_limit: int,
    cap_policy: str = "expand",
) -> dict[str, Any]:
    """生成各分层入选配额。

    规则：
      1. 每层先按比例取，但至少 1 条；
      2. 总配额不超过上限时直接采用；
      3. 超过上限时先保底每层 1 条，剩余按比例分配；
      4. 上限连“每层 1 条”都放不下时有两种取舍：
         - expand（默认）：扩大有效上限到分层数，不突破上限字段的语义；
         - compress：上限保持不变，允许总量超出上限（记录 limit_overrun），
           保证每层仍有一份可追踪样本。
    """
    policy = cap_policy if cap_policy in ("expand", "compress") else "expand"
    order = list(sizes.keys())
    stratum_count = len(order)
    try:
        limit = int(requested_limit)
    except (ValueError, TypeError):
        limit = 0
    limit = max(limit, 0)

    if stratum_count == 0:
        return {
            "allocation": {},
            "requested_limit": limit,
            "effective_limit": limit,
            "cap_expanded": False,
            "cap_policy": policy,
            "limit_overrun": 0,
        }

    desired = {
        key: min(sizes[key], max(1, int(sizes[key] * max(ratio, 0.0))))
        for key in order
    }

    allocation = {key: 0 for key in order}
    cap_expanded = False
    limit_overrun = 0
    if limit < stratum_count:
        # 上限不足以覆盖全部非空分层：保底每层一份
        allocation = {key: 1 for key in order}
        if policy == "expand":
            effective_limit = stratum_count
            cap_expanded = True
        else:
            effective_limit = limit
            limit_overrun = stratum_count - limit
    elif sum(desired.values()) <= limit:
        allocation = dict(desired)
        effective_limit = limit
    else:
        allocation = {key: 1 for key in order}
        _distribute_remaining(allocation, sizes, limit - stratum_count, order)
        effective_limit = limit

    # 二次校验：任何非空分层都至少有一条，且不超过可用量
    for key in order:
        allocation[key] = max(1, min(allocation[key], sizes[key]))

    return {
        "allocation": allocation,
        "requested_limit": limit,
        "effective_limit": effective_limit,
        "cap_expanded": cap_expanded,
        "cap_policy": policy,
        "limit_overrun": limit_overrun,
    }


# ---------------------------------------------------------------------------
# 采样与样本报告
# ---------------------------------------------------------------------------

def _build_strata_report(
    strata: dict[str, list[dict[str, Any]]],
    allocation: dict[str, int],
    desired: dict[str, int],
) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for key, items in strata.items():
        quota = allocation.get(key, 0)
        reports.append({
            "stratum": key,
            "available": len(items),
            "requested": desired.get(key, quota),
            "selected": quota,
            "loss": len(items) - quota,
            "truncated": quota < len(items),
            "covered": quota > 0,
            "selected_ids": [],
        })
    return reports


def stratified_sample(
    events: list[dict[str, Any]],
    group_key: str,
    ratio: float,
    max_samples: int,
    seed_material: str = "",
    cap_policy: str = "expand",
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """执行分层采样并返回 (样本列表, 样本报告)。"""
    unique, duplicates, strata = dedupe_and_group(events, group_key)
    sizes = {key: len(items) for key, items in strata.items()}
    plan = plan_allocation(sizes, ratio, max_samples, cap_policy)
    allocation = plan["allocation"]
    desired = {
        key: min(sizes[key], max(1, int(sizes[key] * max(ratio, 0.0))))
        for key in sizes
    }

    strata_report = _build_strata_report(strata, allocation, desired)
    sampled: list[dict[str, Any]] = []
    position = {id(item): index for index, item in enumerate(unique)}

    for report in strata_report:
        key = report["stratum"]
        items = strata[key]
        quota = allocation[key]
        rng = random.Random(
            stable_seed(seed_material, group_key, key, len(items), quota)
        )
        chosen_indexes = sorted(rng.sample(range(len(items)), quota)) if quota else []
        chosen = [items[index] for index in chosen_indexes]
        report["selected_ids"] = [event_identity(item) for item in chosen]
        report["sample_events"] = [
            item.get("event_id", event_identity(item)) for item in chosen
        ]
        sampled.extend(chosen)

    # 按源事件顺序输出，便于复核与追溯
    sampled.sort(key=lambda item: position.get(id(item), 0))

    uncovered = [
        report["stratum"] for report in strata_report
        if report["available"] > 0 and report["selected"] == 0
    ]
    truncated = [report["stratum"] for report in strata_report if report["truncated"]]
    total_loss = sum(report["loss"] for report in strata_report)
    covered = sum(1 for report in strata_report if report["selected"] > 0)

    report = {
        "issue": ISSUE_ID,
        "result_version": RESULT_VERSION.get(plan["cap_policy"], "issue17.expand.v1"),
        "method": "stratified",
        "group_key": group_key,
        "ratio": ratio,
        "requested_limit": plan["requested_limit"],
        "effective_limit": plan["effective_limit"],
        "cap_expanded": plan["cap_expanded"],
        "cap_policy": plan["cap_policy"],
        "limit_overrun": plan["limit_overrun"],
        "raw_event_count": len(events or []),
        "total_events": len(unique),
        "duplicates_dropped": duplicates,
        "strata_total": len(strata_report),
        "strata_covered": covered,
        "sampled_total": len(sampled),
        "truncated_strata": truncated,
        "truncated_total": total_loss,
        "uncovered_strata": uncovered,
        "coverage_complete": not uncovered,
        "strata": strata_report,
    }
    return sampled, report


def random_sample(
    events: list[dict[str, Any]],
    ratio: float,
    max_samples: int,
    seed_material: str = "",
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """随机采样同样做去重，并复用统一的样本报告结构。"""
    unique, duplicates, _ = dedupe_and_group(events, None)
    try:
        limit = int(max_samples)
    except (ValueError, TypeError):
        limit = len(unique)
    limit = max(limit, 0)
    count = min(max(1, int(len(unique) * max(ratio, 0.0))), limit, len(unique))
    if not unique:
        count = 0
    rng = random.Random(stable_seed(seed_material, "random", len(unique), count))
    sampled = sorted(rng.sample(range(len(unique)), count)) if count else []
    chosen = [unique[index] for index in sampled]
    report = {
        "issue": ISSUE_ID,
        "result_version": RESULT_VERSION["none"],
        "method": "random",
        "group_key": "",
        "ratio": ratio,
        "requested_limit": limit,
        "effective_limit": limit,
        "cap_expanded": False,
        "cap_policy": "none",
        "limit_overrun": 0,
        "raw_event_count": len(events or []),
        "total_events": len(unique),
        "duplicates_dropped": duplicates,
        "strata_total": 0,
        "strata_covered": 0,
        "sampled_total": len(chosen),
        "truncated_strata": [],
        "truncated_total": len(unique) - len(chosen),
        "uncovered_strata": [],
        "coverage_complete": True,
        "strata": [],
        "sampled_ids": [event_identity(item) for item in chosen],
    }
    return chosen, report


# ---------------------------------------------------------------------------
# 复核工具（调试追踪模块也会使用）
# ---------------------------------------------------------------------------

def sampled_stratum_counts(
    sampled: list[dict[str, Any]], group_key: str | None,
) -> Counter:
    return Counter(stratum_of(item, group_key) for item in sampled or [])
