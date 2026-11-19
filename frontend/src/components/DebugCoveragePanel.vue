<template>
  <section class="panel" data-testid="trace-coverage">
    <header class="panel-head">
      <div>
        <h3>调试异常结论复核（序号17）</h3>
        <p class="panel-desc">
          生成异常结论前先核对分层样本数据与样本报告；任一分层缺失即阻止结论，不把未抽样层当作正常。
        </p>
      </div>
      <button class="btn" data-testid="coverage-refresh" type="button" @click="reload">刷新复核</button>
    </header>

    <div class="analysis-form">
      <label>
        调试会话
        <select v-model="session" data-testid="coverage-session" @change="reload">
          <option v-for="name in sessionOptions" :key="name" :value="name">{{ name }}</option>
        </select>
      </label>
      <label>
        绑定采样批次
        <select v-model.number="batchId" data-testid="coverage-batch">
          <option :value="0">最近批次</option>
          <option v-for="item in batches" :key="item.batch_id" :value="item.batch_id">
            批次 {{ item.batch_id }} · {{ item.sampler }}
          </option>
        </select>
      </label>
      <button class="btn" data-testid="coverage-attach" type="button" @click="attach">绑定</button>
      <button class="btn primary" data-testid="coverage-complete" type="button" :disabled="completing" @click="complete">
        {{ completing ? '生成中…' : '生成异常结论' }}
      </button>
    </div>

    <p v-if="errorMessage" class="error-text" data-testid="coverage-error">{{ errorMessage }}</p>
    <p v-if="blockedMessage" class="blocked" data-testid="coverage-blocked">{{ blockedMessage }}</p>
    <p v-if="conclusion" class="notice" data-testid="coverage-conclusion">{{ conclusion }}</p>

    <template v-if="coverage">
      <div class="analysis-summary">
        <article class="analysis-card">
          <span class="stat-label">分层覆盖</span>
          <strong class="stat-value">{{ coverage.strata_covered }} / {{ coverage.strata_total }}</strong>
        </article>
        <article class="analysis-card">
          <span class="stat-label">缺失分层</span>
          <strong class="stat-value">{{ coverage.missing_strata.length }}</strong>
        </article>
        <article class="analysis-card">
          <span class="stat-label">被截断分层</span>
          <strong class="stat-value">{{ coverage.truncated_strata.length }}</strong>
        </article>
        <article class="analysis-card">
          <span class="stat-label">复核结论</span>
          <strong class="stat-value">{{ coverage.ok ? '可生成' : '已阻止' }}</strong>
        </article>
      </div>
      <p class="panel-desc" data-testid="coverage-message">{{ coverage.message }}</p>

      <table class="data-table" data-testid="coverage-table">
        <thead>
          <tr><th>分层</th><th>可用事件</th><th>入选数量</th><th>截断损失</th><th>状态</th></tr>
        </thead>
        <tbody>
          <tr
            v-for="row in coverage.strata"
            :key="row.stratum"
            :data-testid="`coverage-stratum-${row.stratum}`"
            :data-covered="row.covered"
          >
            <td>{{ row.stratum }}</td>
            <td>{{ row.available }}</td>
            <td>{{ row.selected }}</td>
            <td>{{ row.loss }}</td>
            <td>
              <span v-if="!row.covered" class="bad">缺失，禁止结论</span>
              <span v-else-if="row.truncated" class="warn">已截断但可追踪</span>
              <span v-else>完整</span>
            </td>
          </tr>
        </tbody>
      </table>
    </template>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'

import {
  attachSession,
  completeSession,
  loadBatches,
  loadCoverage,
  type CoverageResult,
  type SampleBatch,
} from '@/api/sampling'

const props = defineProps<{ moduleKey: string }>()

const sessionOptions = [
  '调试追踪样例1',
  '调试追踪样例2',
  '调试追踪样例3',
  'trace_window_agg',
]
const session = ref('调试追踪样例1')
const batchId = ref(0)
const batches = ref<SampleBatch[]>([])
const coverage = ref<CoverageResult | null>(null)
const errorMessage = ref('')
const blockedMessage = ref('')
const conclusion = ref('')
const completing = ref(false)

async function reload() {
  errorMessage.value = ''
  blockedMessage.value = ''
  try {
    coverage.value = await loadCoverage(session.value)
  } catch (error) {
    coverage.value = null
    errorMessage.value = error instanceof Error ? error.message : '分层覆盖复核失败'
  }
}

async function reloadBatches() {
  try {
    batches.value = (await loadBatches()).items
  } catch {
    batches.value = []
  }
}

async function attach() {
  errorMessage.value = ''
  try {
    const result = await attachSession(session.value, batchId.value || undefined)
    if (!result.ok) throw new Error(result.error ?? '绑定失败')
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '绑定失败'
  }
}

async function complete() {
  completing.value = true
  errorMessage.value = ''
  blockedMessage.value = ''
  conclusion.value = ''
  try {
    const result = await completeSession(session.value)
    if (result.ok) {
      const analysis = result.analysis as Record<string, unknown>
      const truncated = (analysis.truncated_strata as string[] | undefined) ?? []
      conclusion.value = [
        `结论：${String(analysis.anomaly_type ?? 'none')}`,
        `覆盖 ${String(analysis.strata_covered ?? 0)}/${String(analysis.strata_total ?? 0)} 层`,
        truncated.length ? `被截断分层：${truncated.join('、')}（损失 ${String(analysis.truncated_loss ?? 0)}）` : '无截断',
      ].join('；')
    } else if (result.blocked) {
      blockedMessage.value = result.error
      if (result.coverage) coverage.value = result.coverage
    } else {
      throw new Error(result.error)
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '生成异常结论失败'
  } finally {
    completing.value = false
  }
}

watch(() => props.moduleKey, () => {
  blockedMessage.value = ''
  conclusion.value = ''
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
.panel-desc { color: var(--muted); font-size: 12px; margin: 4px 0 10px; }
.notice { background: #ecfdf3; border: 1px solid #abefc6; color: #067647; border-radius: 6px; padding: 8px 10px; font-size: 12px; margin: 8px 0; }
.blocked { background: #fef3f2; border: 1px solid #fecdca; color: #b42318; border-radius: 6px; padding: 8px 10px; font-size: 12px; margin: 8px 0; }
.bad { color: #b42318; }
.warn { color: #b54708; }
select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; }
</style>
