/** 序号17 专用接口：分层采样、样本报告、调试覆盖复核。 */
import { request } from './client'

export type StratumRow = {
  stratum: string
  available: number
  selected: number
  loss: number
  truncated: boolean
  covered: boolean
}

export type SampleReport = {
  batch_id: number | null
  sampler: string
  method: string
  group_key: string
  ratio: number
  requested_limit: number
  effective_limit: number
  cap_expanded: boolean
  cap_policy: string
  limit_overrun: number
  result_version: string
  total_events: number
  duplicates_dropped: number
  strata_total: number
  strata_covered: number
  sampled_total: number
  truncated_strata: string[]
  truncated_total: number
  uncovered_strata: string[]
  coverage_complete: boolean
  strata: (StratumRow & { selected_ids?: string[]; requested?: number })[]
}

export type SampleBatch = {
  batch_id: number
  sampler: string
  method: string
  group_key: string
  sampled: number
  strata_total: number
  strata_covered: number
  truncated_total: number
  sampled_at: number | null
}

export type CoverageResult = {
  ok: boolean
  batch_id?: number
  sampler?: string
  reason?: string
  message: string
  missing_strata: string[]
  count_mismatch: string[]
  truncated_strata: string[]
  strata_total: number
  strata_covered: number
  strata: StratumRow[]
}

export type SampleRunResult = {
  ok: boolean
  error?: string
  batch_id?: number
  sampled?: number
  total?: number
  strata_total?: number
  strata_covered?: number
  truncated_strata?: string[]
  truncated_total?: number
  duplicates_dropped?: number
  request_duplicates?: number
  cap_expanded?: boolean
  cap_policy?: string
  limit_overrun?: number
  result_version?: string
  deduplicated?: boolean
  coverage_complete?: boolean
}

async function read<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await request(path, init)
  if (!response.ok) {
    let detail = `接口返回 ${response.status}`
    try {
      const body = (await response.json()) as { detail?: unknown }
      if (typeof body.detail === 'string') detail = body.detail
      else if (body.detail) detail = JSON.stringify(body.detail)
    } catch {
      /* 保留状态码提示 */
    }
    throw new Error(detail)
  }
  return (await response.json()) as T
}

export function sampleStratified(values: {
  sampler: string
  events: Record<string, unknown>[]
  method?: string
  group_key?: string
  ratio?: number
  max_samples?: number
  cap_policy?: 'expand' | 'compress'
  idempotency_key?: string
}): Promise<SampleRunResult> {
  return read<SampleRunResult>('/api/sampling/sample', {
    method: 'POST',
    body: JSON.stringify(values),
  })
}

export function loadSampleReport(batchId?: number): Promise<SampleReport> {
  const query = batchId ? `?batch_id=${batchId}` : ''
  return read<SampleReport>(`/api/sampling/report${query}`)
}

export function loadBatches(): Promise<{ items: SampleBatch[] }> {
  return read<{ items: SampleBatch[] }>('/api/sampling/batches')
}

export function attachSession(session: string, batchId?: number): Promise<{ ok: boolean; error?: string }> {
  return read('/api/tracing/attach', {
    method: 'POST',
    body: JSON.stringify({ session, batch_id: batchId ?? null }),
  })
}

export function loadCoverage(session: string): Promise<CoverageResult> {
  return read<CoverageResult>(`/api/tracing/coverage?session=${encodeURIComponent(session)}`)
}

export async function completeSession(session: string): Promise<
  | { ok: true; analysis: Record<string, unknown> }
  | { ok: false; blocked?: boolean; coverage?: CoverageResult; error: string }
> {
  const response = await request('/api/tracing/complete', {
    method: 'POST',
    body: JSON.stringify({ session }),
  })
  const body = await response.json()
  if (response.status === 409) {
    const detail = body.detail ?? {}
    return { ok: false, blocked: true, coverage: detail.coverage, error: detail.error ?? '分层覆盖不完整' }
  }
  if (!response.ok) {
    return { ok: false, error: detailText(body) }
  }
  return body
}

function detailText(body: unknown): string {
  if (body && typeof body === 'object' && 'detail' in body) {
    const detail = (body as { detail: unknown }).detail
    return typeof detail === 'string' ? detail : JSON.stringify(detail)
  }
  return '接口返回异常'
}
