<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Delete } from '@element-plus/icons-vue'
import { api, fmt, percent, date, refresh } from '../composables/useCluster'
import StatusBadge from './StatusBadge.vue'
const props = defineProps<{ jobs: any[] }>()
const router = useRouter()
const selectedIds = ref<string[]>([])
const deletableJobs = computed(() => props.jobs.filter(job => job.status !== 'preparing'))
const selectedJobs = computed(() => props.jobs.filter(job => selectedIds.value.includes(job.id)))
const allSelected = computed(() => deletableJobs.value.length > 0 &&
  deletableJobs.value.every(job => selectedIds.value.includes(job.id)))
const partlySelected = computed(() => selectedJobs.value.length > 0 && !allSelected.value)

watch(() => props.jobs.map(job => job.id), ids => {
  const visible = new Set(ids)
  selectedIds.value = selectedIds.value.filter(id => visible.has(id))
})

function selected(job: any) { return selectedIds.value.includes(job.id) }
function toggle(job: any, checked: boolean) {
  if (job.status === 'preparing') return
  const values = new Set(selectedIds.value)
  if (checked) values.add(job.id); else values.delete(job.id)
  selectedIds.value = [...values]
}
function toggleAll(checked: boolean) {
  const values = new Set(selectedIds.value)
  for (const job of deletableJobs.value) {
    if (checked) values.add(job.id); else values.delete(job.id)
  }
  selectedIds.value = [...values]
}

async function removeMany(jobs: any[]) {
  if (!jobs.length) return
  const batch = jobs.length > 1
  try {
    await ElMessageBox.confirm(
      batch
        ? `批量删除已选的 ${jobs.length} 个任务？\n运行中的任务会在节点下一次心跳时停止，工作包和结果文件会一起删除，不能恢复。`
        : `删除任务「${jobs[0].name}」？\n如果节点正在执行，将在下一次心跳立即停止。工作包和结果文件会一起删除，不能恢复。`,
      batch ? '批量删除任务' : '删除任务',
      { type: 'warning', confirmButtonText: batch ? `删除 ${jobs.length} 个任务` : '删除并停止任务', cancelButtonText: '取消' })
  } catch { return }
  let deleted = 0, stopped = 0
  const failed: string[] = []
  for (const job of jobs) {
    try {
      const result = await api(`/api/jobs/${job.id}`, undefined, 'DELETE')
      deleted += 1
      stopped += Number(result.cancelled_units || 0)
      selectedIds.value = selectedIds.value.filter(id => id !== job.id)
    } catch (exc: any) { failed.push(`${job.name}：${exc?.message || '删除失败'}`) }
  }
  if (deleted) ElMessage.success(batch
    ? `已删除 ${deleted} 个任务${stopped ? `，已通知节点停止 ${stopped} 个在途工作包` : ''}`
    : stopped ? `任务已删除，已通知节点停止 ${stopped} 个在途工作包` : '任务已删除')
  if (failed.length) ElMessage.error(`${failed.length} 个任务删除失败：${failed.slice(0, 2).join('；')}`)
  await refresh()
}
async function remove(job: any) { await removeMany([job]) }
</script>
<template>
  <div v-if="jobs.length" class="job-bulk-toolbar">
    <el-checkbox :model-value="allSelected" :indeterminate="partlySelected" @change="toggleAll(Boolean($event))">全选当前列表</el-checkbox>
    <span class="muted">已选 {{ selectedJobs.length }} 个<span v-if="jobs.length !== deletableJobs.length">（文件准备中不可删除）</span></span>
    <el-button type="danger" plain :icon="Delete" :disabled="!selectedJobs.length" @click="removeMany(selectedJobs)">批量删除</el-button>
  </div>
  <el-table :data="jobs" stripe class="job-table desktop-job-table" empty-text="暂无任务，点击右上角创建任务">
    <el-table-column width="54" align="center"><template #default="{ row }"><el-checkbox :model-value="selected(row)" :disabled="row.status === 'preparing'" :aria-label="`选择任务 ${row.name}`" @change="toggle(row, Boolean($event))" /></template></el-table-column>
    <el-table-column label="任务名称" min-width="250"><template #default="{ row }"><el-button link type="primary" class="job-link" @click="router.push('/jobs/' + row.id)">{{ row.name }}</el-button><div class="muted table-meta">{{ date(row.created) }} · {{ row.mode === 'auth' ? '账号校验' : '数据任务' }}</div></template></el-table-column>
    <el-table-column label="状态" width="185"><template #default="{ row }"><StatusBadge :status="row.status" :unit-counts="row.unit_counts" /><div v-if="row.queue_position" class="muted table-meta">等待第 {{ row.queue_position }} 位</div></template></el-table-column>
    <el-table-column label="任务进度" min-width="220"><template #default="{ row }"><div class="progress-label">{{ fmt(Math.max(row.processed || 0, row.confirmed || 0)) }} / {{ fmt(row.rows) }} 条已处理</div><el-progress :percentage="Number(percent(row).toFixed(1))" :stroke-width="7" /></template></el-table-column>
    <el-table-column label="操作" width="210" fixed="right"><template #default="{ row }">
      <el-button plain @click="router.push('/jobs/' + row.id)">查看详情</el-button>
      <el-tooltip :disabled="row.status !== 'preparing'" content="文件准备完成后可删除" placement="top">
        <span><el-button :icon="Delete" :disabled="row.status === 'preparing'" @click="remove(row)" /></span>
      </el-tooltip>
    </template></el-table-column>
  </el-table>
  <div class="mobile-job-list">
    <div v-if="!jobs.length" class="mobile-job-empty">暂无任务，点击上方按钮创建任务</div>
    <article v-for="row in jobs" :key="row.id" class="mobile-job-card">
      <div class="mobile-job-head">
        <el-checkbox class="mobile-job-selector" :model-value="selected(row)" :disabled="row.status === 'preparing'" :aria-label="`选择任务 ${row.name}`" @change="toggle(row, Boolean($event))" />
        <div class="mobile-job-name"><el-button link type="primary" class="job-link" @click="router.push('/jobs/' + row.id)">{{ row.name }}</el-button><div class="muted table-meta">{{ date(row.created) }} · {{ row.mode === 'auth' ? '账号校验' : '数据任务' }}</div></div>
        <div class="mobile-job-status"><StatusBadge :status="row.status" :unit-counts="row.unit_counts" /><span v-if="row.queue_position" class="muted">等待第 {{ row.queue_position }} 位</span></div>
      </div>
      <div class="mobile-job-progress"><div class="progress-label">{{ fmt(Math.max(row.processed || 0, row.confirmed || 0)) }} / {{ fmt(row.rows) }} 条已处理</div><el-progress :percentage="Number(percent(row).toFixed(1))" :stroke-width="7" /></div>
      <div class="mobile-job-actions">
        <el-button type="primary" plain @click="router.push('/jobs/' + row.id)">查看详情</el-button>
        <el-tooltip :disabled="row.status !== 'preparing'" content="文件准备完成后可删除" placement="top"><span><el-button class="mobile-delete-button" :icon="Delete" aria-label="删除任务" :disabled="row.status === 'preparing'" @click="remove(row)" /></span></el-tooltip>
      </div>
    </article>
  </div>
</template>
<style scoped>
.job-bulk-toolbar{display:flex;align-items:center;gap:14px;min-height:44px;margin-bottom:12px;padding:8px 12px;border:1px solid var(--el-border-color-lighter);border-radius:8px;background:var(--el-fill-color-extra-light)}
.job-bulk-toolbar .el-button{margin-left:auto}.mobile-job-selector{flex:0 0 auto;margin-top:4px}
@media(max-width:760px){.job-bulk-toolbar{flex-wrap:wrap}.job-bulk-toolbar .el-button{width:100%;margin-left:0}.mobile-job-head{align-items:flex-start}}
</style>
