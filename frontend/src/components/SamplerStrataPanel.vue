<template>
  <section class="panel" data-testid="sampler-strata">
    <header class="panel-head">
      <div>
        <h3>分层样本复核（序号17）</h3>
        <p class="panel-desc">
          每层至少保留一份可追踪样本；被截断的分层与损失会写入样本报告，不再只保留前几层。
        </p>
      </div>
      <div class="panel-actions">
        <select v-model.number="batchId" data-testid="strata-batch" @change="reload">
          <option :value="0">最近批次</option>
          <option v-for="item in batches" :key="item.batch_id" :value="item.batch_id">
            批次 {{ item.batch_id }} · {{ item.sampler }} · {{ item.sampled }} 份
          </option>
        </select>
        <button class="btn" data-testid="strata-refresh" type="button" @click="reload">刷新报告</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="strata-error">{{ errorMessage }}</p>
    <p v-else-if="!report" class="empty-hint">还没有采样批次，先在下方按分层方式执行一次采样。</p>

    <template v-else>
      <div class="analysis-summary" data-testid="strata-summary">
        <article class="analysis-card">
          <span class="stat-label">分层覆盖</span>
          <strong class="stat-value">{{ report.strata_covered }} / {{ report.strata_total }}</strong>
        </article>
        <article class="analysis-card">
          <span class="stat-label">入选样本</span>
          <strong class="stat-value">{{ report.sampled_total }}</strong>
        </article>
        <article class="analysis-card">
          <span class="stat-label">截断损失</span>
          <strong class="stat-value">{{ report.truncated_total }}</strong>
        </article>
        <article class="analysis-card">
          <span class="stat-label">重复事件丢弃</span>
          <strong class="stat-value">{{ report.duplicates_dropped }}</strong>
        </article>
        <article class="analysis-card">
          <span class="stat-label">样本上限</span>
          <strong class="stat-value">
            {{ report.effective_limit }}<em v-if="report.cap_expanded" class="flag">已扩容</em>
            <em v-else-if="report.limit_overrun" class="flag">超出 {{ report.limit_overrun }}</em>
          </strong>
        </article>
      </div>

      <p v-if="report.cap_expanded" class="notice" data-testid="strata-cap-expanded">
        样本上限不足覆盖 {{ report.strata_total }} 个非空分层，已扩容到每层至少一份。
      </p>
      <p v-if="report.truncated_strata.length" class="notice" data-testid="strata-truncated">
        按上限截断的分层：{{ report.truncated_strata.join('、') }}（合计损失 {{ report.truncated_total }} 条事件）
      </p>
      <p class="panel-desc" data-testid="strata-version">
        结果版本：{{ report.result_version }} · 上限策略：{{ report.cap_policy === 'compress' ? '按比例压缩' : '扩容上限' }}
      </p>

      <table class="data-table" data-testid="strata-table">
        <thead>
          <tr>
            <th>分层</th>
            <th>可用事件</th>
            <th>入选数量</th>
            <th>截断损失</th>
            <th>是否覆盖</th>
            <th>示例样本</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="row in report.strata"
            :key="row.stratum"
            :data-testid="`stratum-${row.stratum}`"
            :data-covered="row.covered"
          >
            <td>{{ row.stratum }}</td>
            <td>{{ row.available }}</td>
            <td>{{ row.selected }}</td>
            <td>{{ row.loss }}</td>
            <td>{{ row.covered ? '已覆盖' : '缺失' }}</td>
            <td>{{ (row.selected_ids ?? []).join(', ') || '—' }}</td>
          </tr>
        </tbody>
      </table>
    </template>

    <form class="analysis-form" @submit.prevent="runSample">
      <label>
        采样策略
        <select v-model="form.sampler" data-testid="strata-sampler">
          <option v-for="name in samplerOptions" :key="name" :value="name">{{ name }}</option>
        </select>
      </label>
      <label>
        分组键
        <input v-model="form.groupKey" data-testid="strata-group-key" placeholder="region" />
      </label>
      <label>
        采样比例
        <input v-model.number="form.ratio" data-testid="strata-ratio" type="number" step="0.01" min="0" max="1" />
      </label>
      <label>
        样本上限
        <input v-model.number="form.limit" data-testid="strata-limit" type="number" min="0" />
      </label>
      <label>
        上限不足时
        <select v-model="form.capPolicy" data-testid="strata-cap-policy">
          <option value="expand">扩容样本上限</option>
          <option value="compress">按比例压缩</option>
        </select>
      </label>
      <label>
        每层事件数
        <input v-model.number="form.perStratum" data-testid="strata-per-stratum" type="number" min="1" />
      </label>
      <button class="btn primary" data-testid="strata-run" type="submit" :disabled="running">
        {{ running ? '采样中…' : '执行分层采样' }}
      </button>
    </form>
    <p class="panel-desc">
      演示数据按 {{ form.groupKey || 'region' }} 生成 {{ form.strataCount }} 个分层，每层 {{ form.perStratum }} 条事件；
      同一输入重复点击只会命中同一批次，不会重复累计事件。
    </p>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref, watch } from 'vue'

import {
  loadBatches,
  loadSampleReport,
  sampleStratified,
  type SampleBatch,
  type SampleReport,
} from '@/api/sampling'

const props = defineProps<{ moduleKey: string }>()

const batches = ref<SampleBatch[]>([])
const report = ref<SampleReport | null>(null)
const batchId = ref(0)
const running = ref(false)
const errorMessage = ref('')
const samplerOptions = ref(['数据采样样例1', '数据采样样例2', '数据采样样例3'])

const form = reactive({
  sampler: '数据采样样例1',
  groupKey: 'region',
  ratio: 0.5,
  limit: 4,
  perStratum: 5,
  strataCount: 5,
  capPolicy: 'expand' as 'expand' | 'compress',
})

function buildEvents(): Record<string, unknown>[] {
  const events: Record<string, unknown>[] = []
  for (let i = 0; i < form.perStratum; i += 1) {
    for (let s = 0; s < form.strataCount; s += 1) {
      events.push({
        event_id: `demo_${s}_${i}`,
        topic: 'demo_topic',
        payload: { region: `region_${s}`, value: 10 + i, device: `d${i % 3}` },
        event_time: i * 1.5,
        source: 'demo',
      })
    }
  }
  return events
}

async function reload() {
  errorMessage.value = ''
  try {
    report.value = await loadSampleReport(batchId.value || undefined)
    if (batchId.value === 0 && report.value.batch_id) batchId.value = report.value.batch_id
  } catch (error) {
    report.value = null
    errorMessage.value = error instanceof Error ? error.message : '样本报告读取失败'
  }
}

async function reloadBatches() {
  try {
    batches.value = (await loadBatches()).items
  } catch {
    batches.value = []
  }
}

async function runSample() {
  running.value = true
  errorMessage.value = ''
  try {
    const result = await sampleStratified({
      sampler: form.sampler,
      events: buildEvents(),
      method: 'stratified',
      group_key: form.groupKey || 'region',
      ratio: form.ratio,
      max_samples: form.limit,
      cap_policy: form.capPolicy,
    })
    if (!result.ok) throw new Error(result.error ?? '采样失败')
    batchId.value = result.batch_id ?? 0
    await reloadBatches()
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采样失败'
  } finally {
    running.value = false
  }
}

watch(() => props.moduleKey, () => {
  batchId.value = 0
  void reloadBatches()
  void reload()
})

onMounted(async () => {
  await reloadBatches()
  await reload()
})
</script>

<style scoped>
.panel { margin-top: 16px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
.panel-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
.panel-head h3 { margin: 0; font-size: 15px; }
.panel-actions { display: flex; gap: 8px; align-items: center; }
.panel-desc { color: var(--muted); font-size: 12px; margin: 4px 0 10px; }
.empty-hint { color: var(--muted); font-size: 13px; }
.notice { background: #fff7e6; border: 1px solid #f5d08a; color: #8a5300; border-radius: 6px; padding: 8px 10px; font-size: 12px; margin: 8px 0; }
.flag { font-size: 11px; color: #b54708; margin-left: 6px; font-style: normal; }
select, input { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; }
</style>
