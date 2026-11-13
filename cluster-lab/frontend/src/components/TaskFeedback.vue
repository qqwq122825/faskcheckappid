<script setup lang="ts">
import { computed } from 'vue'
import { fmt } from '../composables/useCluster'
import StatusBadge from './StatusBadge.vue'
const props = defineProps<{
  total: number, completed: number, active: number, success: number,
  failed?: number | null, timeout?: number | null, unconfirmed: number,
  pending: number, rate: number, status: string, preview?: boolean, skipped?: number,
  elapsed?: number | null, categorized?: Record<string, number> | null,
  capacity?: string,
  processed?: number, unitCounts?: Record<string, number>,
}>()
const reportedProcessed = computed(() => props.processed == null ? null : Math.max(props.completed, Math.min(props.total, props.processed)))
const percentage = computed(() => props.total ? Math.min(100, (reportedProcessed.value ?? props.completed) / props.total * 100) : 0)
const awaitingConfirmation = computed(() => reportedProcessed.value == null ? null : Math.max(0, reportedProcessed.value - props.completed))
interface Cell { label: string; value: number | null | undefined; tone?: string; suffix?: string }
const categories = computed(() => props.categorized || {})
const cells = computed<Cell[]>(() => [
  { label: '总数', value: props.total },
  { label: '已确认', value: props.completed,
    tone: props.total && props.completed >= props.total ? 'success' : undefined },
  { label: '剩余', value: Math.max(0, props.total - (reportedProcessed.value ?? props.completed)) },
  { label: '在途记录', value: props.active },
  { label: '密正', value: categories.value.valid ?? 0, tone: 'success' },
  { label: '密码错误', value: categories.value.wrong_password ?? 0 },
  { label: '双重验证', value: categories.value.need_2fa ?? 0 },
  { label: '锁定', value: categories.value.locked ?? 0 },
  { label: '超时', value: categories.value.timeout ?? 0, tone: 'warning' },
  { label: '未判断', value: categories.value.unknown ?? 0 },
])
</script>
<template>
  <el-card shadow="never" class="task-feedback">
    <template #header><div class="section-heading"><h2>进度与结果</h2><div class="feedback-state">
      <el-tag v-if="preview" type="warning" effect="plain">合成演示</el-tag>
      <el-tag v-if="preview" :type="status === 'completed' ? 'success' : status === 'paused' ? 'warning' : 'primary'" round>{{ status === 'completed' ? '已完成' : status === 'paused' ? '已暂停' : '运行中' }}</el-tag>
      <StatusBadge v-else :status="status" :unit-counts="unitCounts" />
    </div></div></template>
    <div class="feedback-grid"><div v-for="cell in cells" :key="cell.label" class="feedback-cell" :class="cell.tone"><span>{{ cell.label }}</span><strong>{{ cell.value == null ? '—' : fmt(cell.value) }}<small v-if="cell.suffix">{{ cell.suffix }}</small></strong></div></div>
    <div class="feedback-footer">
      <div class="feedback-progress-label"><span>实时已处理 {{ fmt(reportedProcessed ?? completed) }} / {{ fmt(total) }}</span><strong>{{ percentage.toFixed(1) }}%</strong></div>
      <el-progress :percentage="Number(percentage.toFixed(1))" :show-text="false" :stroke-width="10" /><div v-if="$slots.actions" class="feedback-actions"><slot name="actions" /></div>
      <div v-if="!preview" class="feedback-meta" aria-label="任务进度口径">
        <span>节点已处理 {{ reportedProcessed == null ? '尚未上报' : fmt(reportedProcessed) + ' 条' }}</span>
        <span>总控已确认 {{ fmt(completed) }} 条</span>
        <span>已处理待确认 {{ awaitingConfirmation == null ? '—' : fmt(awaitingConfirmation) + ' 条' }}</span>
      </div>
      <p v-if="!preview && active > 0 && !reportedProcessed">等待节点上报中间进度；在途记录数不代表同时执行数量。</p>
      <div class="feedback-meta"><span v-if="capacity">工作进程 {{ capacity }}</span><span>用时 {{ elapsed == null ? '—' : fmt(elapsed) }} 秒 · {{ fmt(rate) }} 条/小时</span></div>
      <p>{{ preview ? '合成统计，不代表真实任务结果。' : '进度和分类按节点每秒上报实时更新；运行中的分类尚待确认，完成状态仍以总控确认全部结果为准。' }}</p>
    </div>
  </el-card>
</template>
