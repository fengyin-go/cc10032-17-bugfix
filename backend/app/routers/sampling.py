"""序号17 专用接口：分层采样、样本报告复核、调试异常结论。

放在通用 /api/{module} 路由之后注册，路径固定前缀 /api/sampling 与 /api/tracing，
不会被动态模块路径抢先匹配。
"""
from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services import service_for

router = APIRouter(prefix="/api", tags=["序号17 采样与调试"])


class SampleRequest(BaseModel):
    sampler: str
    events: list[dict[str, Any]] = Field(default_factory=list)
    idempotency_key: str | None = None
    dry_run: bool = False
    # 可选的本次采样参数覆盖，便于页面直接演示不同的分层键/上限
    method: str | None = None
    group_key: str | None = None
    ratio: float | None = None
    max_samples: int | None = None
    # 上限不足以覆盖全部非空分层时的取舍：expand（扩容上限）/ compress（保持上限、按比例压缩）
    cap_policy: Literal["expand", "compress"] = "expand"


class AttachRequest(BaseModel):
    session: str
    batch_id: int | None = None


class ReadSourceRequest(BaseModel):
    source: str
    batch_size: int = 20


class CompleteRequest(BaseModel):
    session: str


def _sampler():
    service = service_for("data_sampler")
    if service is None:  # pragma: no cover - 注册表固定，不会发生
        raise HTTPException(status_code=500, detail="数据采样服务未注册")
    return service


def _tracer():
    service = service_for("debug_trace")
    if service is None:  # pragma: no cover
        raise HTTPException(status_code=500, detail="调试追踪服务未注册")
    return service


@router.post("/sampling/sample")
def run_sampling(payload: SampleRequest) -> dict[str, Any]:
    """执行一次分层采样，返回样本报告摘要（含截断分层与去重统计）。"""
    return _sampler().apply_sampling(
        payload.sampler,
        payload.events,
        idempotency_key=payload.idempotency_key,
        dry_run=payload.dry_run,
        method=payload.method,
        group_key=payload.group_key,
        ratio=payload.ratio,
        max_samples=payload.max_samples,
        cap_policy=payload.cap_policy,
    )


@router.post("/sampling/source-events")
def read_source_events(payload: ReadSourceRequest) -> dict[str, Any]:
    """从流式源端读取采样事件；源端异常断连时直接报错，不生成样本。"""
    result = _sampler().read_events_from_source(payload.source, payload.batch_size)
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result.get("error"))
    return result


@router.get("/sampling/ledger")
def fix_ledger(limit: int = Query(default=50, ge=1, le=200)) -> dict[str, Any]:
    """序号17 修复台账：其他模块同步更新修复记录时读取。"""
    return {"items": _sampler().fix_ledger(limit)}


@router.get("/sampling/batches")
def list_batches(limit: int = Query(default=20, ge=1, le=200)) -> dict[str, Any]:
    """列出最近采样批次，供页面选择要复核/绑定的批次。"""
    return {"items": _sampler().list_batches(limit)}


@router.get("/sampling/report")
def get_sample_report(batch_id: int | None = Query(default=None)) -> dict[str, Any]:
    """读取样本报告：每层入选数量、截断损失、是否扩容上限。"""
    report = _sampler().get_sample_report(batch_id)
    if report is None:
        raise HTTPException(status_code=404, detail="暂无采样批次，请先执行采样")
    return report


@router.get("/sampling/strata")
def list_strata(batch_id: int | None = Query(default=None)) -> dict[str, Any]:
    """采样页面使用：查看每层入选数量与截断情况。"""
    service = _sampler()
    counts = service.list_stratum_counts(batch_id)
    report = service.get_sample_report(batch_id)
    if report is None:
        raise HTTPException(status_code=404, detail="暂无采样批次，请先执行采样")
    return {
        "batch_id": report.get("batch_id"),
        "sampled": report.get("sampled", 0),
        "strata_total": report.get("strata_total", 0),
        "strata_covered": report.get("strata_covered", 0),
        "truncated_strata": report.get("truncated_strata", []),
        "truncated_total": report.get("truncated_total", 0),
        "cap_expanded": report.get("cap_expanded", False),
        "cap_policy": report.get("cap_policy", "none"),
        "limit_overrun": report.get("limit_overrun", 0),
        "result_version": report.get("result_version", ""),
        "duplicates_dropped": report.get("duplicates_dropped", 0),
        "strata": counts,
    }


@router.post("/tracing/attach")
def attach_batch(payload: AttachRequest) -> dict[str, Any]:
    """把调试会话绑定到采样批次。"""
    return _tracer().attach_sampling_batch(payload.session, payload.batch_id)


@router.get("/tracing/coverage")
def coverage(session: str = Query(...)) -> dict[str, Any]:
    """调试页面使用：核对分层样本数据与样本报告，返回缺失/截断分层。"""
    result = _tracer().check_sample_coverage(session)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.post("/tracing/complete")
def complete(payload: CompleteRequest) -> dict[str, Any]:
    """完成调试会话；分层覆盖不完整时返回 409，阻止生成异常结论。"""
    result = _tracer().complete_session_with_analysis(payload.session)
    if result.get("blocked"):
        raise HTTPException(status_code=409, detail=result)
    if not result.get("ok") and result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.post("/tracing/analyze")
def analyze(payload: CompleteRequest) -> dict[str, Any]:
    """查看异常模式；分层覆盖不完整时返回 409，阻止异常结论。"""
    result = _tracer().analyze_anomaly_patterns(payload.session)
    if result.get("blocked"):
        raise HTTPException(status_code=409, detail=result)
    if not result.get("ok") and result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])
    return result
