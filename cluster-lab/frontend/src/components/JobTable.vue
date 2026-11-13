<script setup lang="ts">
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Delete } from '@element-plus/icons-vue'
import { api, fmt, percent, date, refresh } from '../composables/useCluster'
import StatusBadge from './StatusBadge.vue'
defineProps<{ jobs: any[] }>()
const router = useRouter()
const removable = ['completed', 'failed', 'stopped', 'paused']

async function remove(job: any) {
  try {
    await ElMessageBox.confirm(
      `删除任务「${job.name}」？\n工作包与已生成的结果文件会一起删除，不能恢复。`,
      '删除任务', { type: 'warning', confirmButtonText: '删除', cancelButtonText: '取消' })
  } catch { return }
  try {
    const result = await api(`/api/jobs/${job.id}`, undefined, 'DELETE')
    ElMessage.success(result.source_kept ? '任务已删除（源文件被其他任务复用，已保留）' : '任务已删除')
    await refresh()
  } catch (exc: any) { ElMessage.error(exc?.message || '删除失败') }
}
</script>
<template>
  <el-table :data="jobs" stripe class="job-table" empty-text="暂无任务，点击右上角创建任务">
    <el-table-column label="任务名称" min-width="250"><template #default="{ row }"><el-button link type="primary" class="job-link" @click="router.push('/jobs/' + row.id)">{{ row.name }}</el-button><div class="muted table-meta">{{ date(row.created) }} · {{ row.mode === 'auth' ? '账号校验' : row.mode === 'simulate' ? '离线演示' : '文件摘要' }}</div></template></el-table-column>
    <el-table-column label="状态" width="185"><template #default="{ row }"><StatusBadge :status="row.status" :unit-counts="row.unit_counts" /><div v-if="row.queue_position" class="muted table-meta">等待第 {{ row.queue_position }} 位</div></template></el-table-column>
    <el-table-column label="任务进度" min-width="220"><template #default="{ row }"><div class="progress-label">{{ fmt(Math.max(row.processed || 0, row.confirmed || 0)) }} / {{ fmt(row.rows) }} 条已处理</div><el-progress :percentage="Number(percent(row).toFixed(1))" :stroke-width="7" /></template></el-table-column>
    <el-table-column label="操作" width="210" fixed="right"><template #default="{ row }">
      <el-button plain @click="router.push('/jobs/' + row.id)">查看详情</el-button>
      <el-tooltip :disabled="removable.includes(row.status)" content="任务正在执行，先停止再删除" placement="top">
        <span><el-button :icon="Delete" :disabled="!removable.includes(row.status)" @click="remove(row)" /></span>
      </el-tooltip>
    </template></el-table-column>
  </el-table>
</template>
