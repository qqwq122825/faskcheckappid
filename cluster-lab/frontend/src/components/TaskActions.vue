<script setup lang="ts">
import { ElMessage, ElMessageBox } from 'element-plus'
import { VideoPlay, VideoPause, Download } from '@element-plus/icons-vue'
import { busy, mutate } from '../composables/useCluster'
const props = defineProps<{ job: any }>()
async function action(value: string) {
  try {
    if (value === 'retry_unknown') await ElMessageBox.confirm('仅适用于当前离线可重复任务。将待确认工作包重新排队，可能再次计算已执行但未确认的记录。', '重新排队待确认工作包', { type: 'warning', confirmButtonText: '确认重新排队', cancelButtonText: '取消' })
    await mutate(`/api/jobs/${props.job.id}/${value}`, {}, value === 'pause' ? '已暂停派发，在途工作继续执行' : value === 'start' ? '任务已启动' : '待确认工作包已重新排队')
  } catch (error) { if (error instanceof Error) ElMessage.error(error.message) }
}
</script>
<template><div class="actions"><el-button v-if="['ready', 'paused'].includes(job.status)" type="primary" :icon="VideoPlay" :loading="busy" @click="action('start')">{{ job.status === 'paused' ? '继续任务' : '开始任务' }}</el-button><el-button v-if="job.status === 'running'" :icon="VideoPause" :loading="busy" @click="action('pause')">暂停派发</el-button><el-button v-if="job.unit_counts.unknown" type="warning" plain :disabled="busy" @click="action('retry_unknown')">重新排队待确认包</el-button><el-button v-if="job.status === 'completed'" tag="a" :href="`/api/jobs/${job.id}/results`" :icon="Download" type="primary" plain>导出本次结果</el-button><el-button v-else disabled :icon="Download">全部确认后可导出</el-button></div></template>
