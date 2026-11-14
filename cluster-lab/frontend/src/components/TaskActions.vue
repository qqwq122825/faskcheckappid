<script setup lang="ts">
import { ElMessage, ElMessageBox } from 'element-plus'
import { VideoPlay, VideoPause, Download } from '@element-plus/icons-vue'
import { busy, mutate } from '../composables/useCluster'
const props = defineProps<{ job: any }>()
async function action(value: string) {
  try {
    if (value === 'retry_unknown') await ElMessageBox.confirm('将待确认工作包重新排队，可能再次处理已执行但未确认的记录。', '重新排队待确认工作包', { type: 'warning', confirmButtonText: '确认重新排队', cancelButtonText: '取消' })
    await mutate(`/api/jobs/${props.job.id}/${value}`, {}, value === 'pause' ? '已暂停派发，在途工作继续执行' : value === 'start' ? '已提交执行，按队列顺序处理' : value === 'dequeue' ? '已移出队列' : '待确认工作包已重新排队')
  } catch (error) { if (error instanceof Error) ElMessage.error(error.message) }
}
</script>
<template><div class="actions"><el-button v-if="['ready', 'paused'].includes(job.status)" type="primary" :icon="VideoPlay" :loading="busy" @click="action('start')">{{ job.status === 'paused' ? '继续当前任务' : '加入执行队列' }}</el-button><el-button v-if="job.status === 'queued' || job.status === 'preparing' && job.queued_at" :disabled="busy" @click="action('dequeue')">移出队列</el-button><el-button v-if="job.status === 'running'" :icon="VideoPause" :loading="busy" @click="action('pause')">暂停派发</el-button><el-button v-if="job.unit_counts.unknown" type="warning" plain :disabled="busy" @click="action('retry_unknown')">重新排队待确认包</el-button><el-button v-if="['paused', 'completed'].includes(job.status)" tag="a" :href="`/api/jobs/${job.id}/results`" :icon="Download" type="primary" plain>{{ job.status === 'paused' ? '导出已确认结果' : '导出本次 TXT' }}</el-button><el-button v-else disabled :icon="Download">{{ job.status === 'running' ? '暂停后可导出已确认结果' : '暂停或完成后可导出' }}</el-button></div></template>
