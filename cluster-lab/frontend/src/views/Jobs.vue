<script setup lang="ts">
import { computed, ref } from 'vue'
import { Plus, Search, Delete } from '@element-plus/icons-vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { state, isNode, createTaskOpen, fmt, date, api, refresh } from '../composables/useCluster'
import JobTable from '../components/JobTable.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { matchesJobStatus } from '../lib/jobStatus.js'
const search = ref(''), status = ref('')
const selectedJobs = ref<any[]>([])
const jobTable = ref<any>()
const jobs = computed(() => (state.value?.jobs || []).filter((job: any) => matchesJobStatus(job, status.value) && job.name.toLowerCase().includes(search.value.toLowerCase())))
const selectableJobs = computed(() => jobs.value.filter((job: any) => job.status !== 'preparing'))
const allVisibleSelected = computed(() => selectableJobs.value.length > 0 && selectableJobs.value.every((job: any) => selectedJobs.value.some((selected: any) => selected.id === job.id)))
function onSelectionChange(rows: any[]) { selectedJobs.value = rows }
function toggleSelectAll() {
  if (allVisibleSelected.value) jobTable.value?.clearSelection()
  else jobTable.value?.selectAllVisible()
}

async function batchRemove() {
  if (!selectedJobs.value.length) return
  try {
    await ElMessageBox.confirm(
      `删除选中的 ${selectedJobs.value.length} 个任务？\n正在执行的任务会在下一次心跳立即停止；工作包和结果文件会一起删除，不能恢复。`,
      '批量删除任务', { type: 'warning', confirmButtonText: '删除并停止任务', cancelButtonText: '取消' })
  } catch { return }
  try {
    const result = await api('/api/jobs/batch-delete', { ids: selectedJobs.value.map((job: any) => job.id) }, 'POST')
    const skipped = Number((result.skipped || []).length)
    ElMessage.success(skipped
      ? `已删除 ${result.deleted} 个任务，跳过 ${skipped} 个文件准备中的任务`
      : `已删除 ${result.deleted} 个任务`)
    selectedJobs.value = []
    jobTable.value?.clearSelection()
    await refresh()
  } catch (exc: any) { ElMessage.error(exc?.message || '删除失败') }
}
</script>
<template><el-card shadow="never" class="section-card"><template #header><div class="section-heading"><div><h2>{{ isNode ? '本机工作包' : '全部任务' }}</h2><p>{{ isNode ? '展示最近 60 个本地工作包。' : '优先显示当前和排队任务，最多展示 50 个；新任务按顺序接续执行。' }}</p></div><el-button v-if="!isNode" type="primary" :icon="Plus" @click="createTaskOpen = true">上传 TXT 创建任务</el-button></div></template>
  <template v-if="!isNode"><el-alert class="section-card" :title="`等待队列 ${state?.totals?.queued || 0} 个任务；当前任务全部确认后自动开始下一个。暂停当前任务会保留它的位置。`" type="info" :closable="false" show-icon /><div class="table-toolbar"><el-input v-model="search" clearable :prefix-icon="Search" placeholder="搜索任务名称" aria-label="搜索任务" /><el-select v-model="status" clearable placeholder="全部状态" aria-label="筛选任务状态"><el-option label="运行中" value="running" /><el-option label="异常/待确认" value="attention" /><el-option label="排队中" value="queued" /><el-option label="文件准备中" value="preparing" /><el-option label="未入队" value="ready" /><el-option label="已暂停" value="paused" /><el-option label="已完成" value="completed" /><el-option label="准备失败" value="failed" /></el-select><el-button :disabled="!selectableJobs.length" @click="toggleSelectAll">{{ allVisibleSelected ? '取消全选' : `全选当前列表（${selectableJobs.length}）` }}</el-button><el-button type="danger" plain :icon="Delete" :disabled="!selectedJobs.length" @click="batchRemove">删除选中（{{ selectedJobs.length }}）</el-button></div><JobTable ref="jobTable" :jobs="jobs" @selection-change="onSelectionChange" /></template>
  <el-table v-else :data="state?.tasks || []" stripe empty-text="暂未领取工作包"><el-table-column label="工作包" min-width="180"><template #default="{ row }">工作包 #{{ row.seq }}<div class="muted table-meta">{{ row.job_id.slice(0, 8) }}</div></template></el-table-column><el-table-column label="状态" min-width="160"><template #default="{ row }"><StatusBadge :status="row.status" /><p v-if="row.error" class="error-text">{{ row.error }}</p></template></el-table-column><el-table-column label="执行进度" min-width="180"><template #default="{ row }">{{ fmt(row.processed) }} / {{ fmt(row.rows) }}</template></el-table-column><el-table-column label="更新时间" min-width="200"><template #default="{ row }">{{ date(row.updated) }}</template></el-table-column></el-table>
</el-card></template>
