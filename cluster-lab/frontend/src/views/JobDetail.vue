<script setup lang="ts">
import { computed, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ArrowLeft, Download } from '@element-plus/icons-vue'
import { state, fmt, date, jobRates, isNode } from '../composables/useCluster'
import TaskFeedback from '../components/TaskFeedback.vue'
import StatusBadge from '../components/StatusBadge.vue'
import TaskActions from '../components/TaskActions.vue'
const route = useRoute(), router = useRouter(), expanded = ref<string[]>([])
const job = computed(() => state.value?.jobs?.find((j: any) => j.id === route.params.id))
const count = (key: string) => job.value?.row_counts?.[key] || 0
// 全程平均速度：任务结束后瞬时速度归零，"0 条/秒"没有参考价值
const elapsedSeconds = computed(() => {
  const start = job.value?.created, end = job.value?.updated
  if (!start || !end || end <= start) return 0
  return Math.max(0, Math.round(end - start))
})
// 参照单机版的展示口径：条/小时
const hourlyRate = computed(() => {
  const span = elapsedSeconds.value
  return span > 0 ? Math.round((job.value?.confirmed || 0) * 3600 / span) : 0
})
const capacityText = computed(() => {
  const active = state.value?.nodes?.filter((n: any) => n.status !== 'offline' && n.status !== 'stopping') || []
  const total = active.reduce((sum: number, n: any) => sum + (n.effective_processes || n.desired_processes || 0), 0)
  if (!total) return ''
  const threads = active[0]?.effective_threads || active[0]?.desired_threads || 0
  return `${active.length} 节点 × ${total} 进程 × 每进程 ${threads} 线程`
})
const processing = computed(() => Math.max(0, count('leased') + count('running') - Math.max(0, (job.value?.processed || 0) - (job.value?.confirmed || 0))))
const waiting = computed(() => Math.max(0, (job.value?.processed || 0) - (job.value?.confirmed || 0)))
const events = computed(() => state.value?.events?.filter((e: any) => e.job_id === route.params.id) || [])

</script>
<template>
  <el-button :icon="ArrowLeft" class="back-button" @click="router.push('/jobs')">返回任务管理</el-button>
  <template v-if="job && !isNode"><el-card shadow="never" class="section-card task-heading"><div class="section-heading"><div><div class="task-name"><h2>{{ job.name }}</h2><StatusBadge :status="job.status" /></div><p>{{ date(job.created) }} 创建 · {{ job.mode === 'simulate' ? '合成离线演示' : '逐行 SHA-256 摘要' }}</p></div><TaskActions :job="job" /></div></el-card>
    <el-alert v-if="job.status === 'queued'" :title="`任务排队中，等待第 ${job.queue_position} 位。轮到后自动开始。`" type="info" :closable="false" show-icon /><el-alert v-if="job.error" :title="job.error" type="error" :closable="false" show-icon />
    <el-alert v-else-if="job.unit_counts.unknown" title="存在待确认工作包，未自动重新执行。请核对后再决定是否重新排队。" type="warning" :closable="false" show-icon />
    <el-alert v-else-if="job.status === 'paused'" title="已暂停派发新工作包，在途工作会继续执行并回传。" type="info" :closable="false" show-icon />
    <el-alert v-else-if="job.status === 'preparing'" title="正在准备文件，当前总数为已解析数量。准备完成后按队列顺序开始。" type="info" :closable="false" show-icon />
    <TaskFeedback :total="job.rows" :completed="job.confirmed" :active="count('leased') + count('running')" :success="job.confirmed" :failed="null" :timeout="null" :unconfirmed="count('unknown') + waiting" :pending="count('pending')" :rate="hourlyRate" :elapsed="elapsedSeconds" :capacity="capacityText" :categorized="job.categorized" :status="job.status" />
    <el-collapse v-model="expanded" class="section-card"><el-collapse-item title="执行明细" name="execution"><el-descriptions :column="3" border><el-descriptions-item label="分片总数">{{ job.units }}</el-descriptions-item><el-descriptions-item label="已确认分片">{{ job.unit_counts.completed || 0 }}</el-descriptions-item><el-descriptions-item label="在途分片">{{ (job.unit_counts.leased || 0) + (job.unit_counts.running || 0) }}</el-descriptions-item><el-descriptions-item label="待分配记录">{{ fmt(count('pending')) }}</el-descriptions-item><el-descriptions-item label="在途未执行">{{ fmt(processing) }}</el-descriptions-item><el-descriptions-item label="已执行待确认">{{ fmt(waiting) }}</el-descriptions-item></el-descriptions><p class="muted">每片最多 {{ fmt(job.unit_rows) }} 条 · 已确认 {{ job.unit_counts.completed || 0 }} / {{ job.units }} 片</p></el-collapse-item></el-collapse>
    <el-card shadow="never" class="section-card"><template #header><div class="section-heading"><div><h2>本次任务导出</h2><p>全部工作包确认后，按上传顺序导出 TXT，每行一条原始记录。</p></div><el-button v-if="job.status === 'completed'" tag="a" :href="`/api/jobs/${job.id}/results`" :icon="Download" type="primary">下载 TXT 结果</el-button><el-button v-else disabled :icon="Download">等待全部确认</el-button></div></template><el-descriptions :column="2" border><el-descriptions-item label="结果内容">TXT 原始记录，保持上传顺序</el-descriptions-item><el-descriptions-item label="文件命名">日期_处理成功_条数.txt（北京时间）</el-descriptions-item><el-descriptions-item label="完整结果">{{ job.status === 'completed' ? '可下载' : '等待全部确认' }}</el-descriptions-item><el-descriptions-item label="任务编号">{{ job.id }}</el-descriptions-item><el-descriptions-item label="工作包数">{{ job.units }}</el-descriptions-item></el-descriptions></el-card>
    <el-collapse v-model="expanded"><el-collapse-item title="查看本次任务的最近操作记录" name="events"><el-timeline v-if="events.length"><el-timeline-item v-for="event in events" :key="event.id" :timestamp="date(event.at)">{{ event.message }}</el-timeline-item></el-timeline><el-empty v-else description="最近记录中没有本任务事件" /></el-collapse-item></el-collapse>
  </template>
  <el-empty v-else description="任务不在最近列表中，或当前页面是节点管理页。" />
</template>
