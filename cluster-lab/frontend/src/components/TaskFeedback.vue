<script setup lang="ts">
import { computed } from 'vue'
import { fmt } from '../composables/useCluster'
import StatusBadge from './StatusBadge.vue'
const props = defineProps<{
  total: number, completed: number, active: number, success: number,
  failed?: number | null, timeout?: number | null, unconfirmed: number,
  pending: number, rate: number, status: string, preview?: boolean, skipped?: number,
}>()
const percentage = computed(() => props.total ? Math.min(100, props.completed / props.total * 100) : 0)
const cells = computed(() => [
  { label: '总数', value: props.total },
  { label: '完成', value: props.completed },
  { label: '剩余', value: Math.max(0, props.total - props.completed) },
  { label: '在途记录', value: props.active },
  { label: '处理成功', value: props.success, tone: 'success' },
  { label: '处理失败', value: props.failed, tone: 'danger' },
  { label: '超时', value: props.timeout, tone: 'warning' },
  { label: props.preview ? '跳过' : '待确认', value: props.preview ? props.skipped : props.unconfirmed },
  { label: '待分配', value: props.pending },
  { label: '处理速度', value: props.rate, suffix: '条 / 秒' },
])
</script>
<template><el-card shadow="never" class="task-feedback"><template #header><div class="section-heading"><h2>进度与结果</h2><div class="feedback-state"><el-tag v-if="preview" type="warning" effect="plain">合成演示</el-tag><el-tag v-if="preview" :type="status === 'completed' ? 'success' : status === 'paused' ? 'warning' : 'primary'" round>{{ status === 'completed' ? '已完成' : status === 'paused' ? '已暂停' : '运行中' }}</el-tag><StatusBadge v-else :status="status" /></div></div></template><div class="feedback-grid"><div v-for="cell in cells" :key="cell.label" class="feedback-cell" :class="cell.tone"><span>{{ cell.label }}</span><strong>{{ cell.value == null ? '—' : fmt(cell.value) }}<small v-if="cell.suffix">{{ cell.suffix }}</small></strong></div></div><div class="feedback-footer"><div class="feedback-progress-label"><span>{{ fmt(completed) }} / {{ fmt(total) }}</span><strong>{{ percentage.toFixed(1) }}%</strong></div><el-progress :percentage="Number(percentage.toFixed(1))" :show-text="false" :stroke-width="10" /><p>{{ preview ? '仅为本地合成统计，不代表账号验证或真实任务结果。' : '完成数以总控确认结果为准；未采集的失败、超时分类显示为 —。' }}</p></div></el-card></template>
