<script setup lang="ts">
import { computed, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ArrowLeft, Download } from '@element-plus/icons-vue'
import { state, fmt, date, jobRates, isNode } from '../composables/useCluster'
import TaskFeedback from '../components/TaskFeedback.vue'
import StatusBadge from '../components/StatusBadge.vue'
import TaskActions from '../components/TaskActions.vue'
import ResultList from '../components/ResultList.vue'
import { jobIssueMessage } from '../lib/jobStatus.js'
const route = useRoute(), router = useRouter(), expanded = ref<string[]>([])
const exportOptions = [
  { label: '全部结果', value: 'all' },
  { label: '密正', value: 'valid' },
  { label: '密码错误', value: 'wrong_password' },
  { label: '双重验证', value: 'need_2fa' },
  { label: '锁定', value: 'locked' },
  { label: '已失效', value: 'invalid' },
  { label: '超时', value: 'timeout' },
  { label: '其他未验证', value: 'undetermined' },
]
function download(category: string) {
  if (!job.value) return
  location.href = `/api/jobs/${job.value.id}/results?category=${category}`
}
const job = computed(() => state.value?.jobs?.find((j: any) => j.id === route.params.id))
const issueMessage = computed(() => jobIssueMessage(job.value))
const count = (key: string) => job.value?.row_counts?.[key] || 0
// 全程平均速度：任务结束后瞬时速度归零，"0 条/秒"没有参考价值
const elapsedSeconds = computed(() => {
  const start = job.value?.created
  const end = job.value?.status === 'running' ? state.value?.server_time : job.value?.updated
  if (!start || !end || end <= start) return 0
  return Math.max(0, Math.round(end - start))
})
// 参照单机版的展示口径：条/小时
const hourlyRate = computed(() => {
  const span = elapsedSeconds.value
  const completed = Math.max(job.value?.processed || 0, job.value?.confirmed || 0)
  return span > 0 ? Math.round(completed * 3600 / span) : 0
})
const capacityText = computed(() => {
  const active = state.value?.nodes?.filter((n: any) => n.status !== 'offline' && n.status !== 'stopping') || []
  const total = active.reduce((sum: number, n: any) => sum + (n.effective_processes || n.desired_processes || 0), 0)
  if (!total) return ''
  const threads = active[0]?.effective_threads || active[0]?.desired_threads || 0
  const threadCounts = new Set(active.map((node: any) => node.effective_threads || node.desired_threads || 0))
  return `${active.length} 节点 · 共 ${total} 进程 · ${threadCounts.size === 1 ? `每进程 ${threads} 线程` : '线程数按各节点配置'}`
})
const processing = computed(() => Math.max(0, count('leased') + count('running') - Math.max(0, (job.value?.processed || 0) - (job.value?.confirmed || 0))))
const waiting = computed(() => Math.max(0, (job.value?.processed || 0) - (job.value?.confirmed || 0)))
const events = computed(() => state.value?.events?.filter((e: any) => e.job_id === route.params.id) || [])

</script>
<template>
  <el-button :icon="ArrowLeft" class="back-button" @click="router.push('/jobs')">返回任务管理</el-button>
  <template v-if="job && !isNode">
    <el-card shadow="never" class="section-card task-heading">
      <div class="section-heading">
        <div>
          <div class="task-name"><h2>{{ job.name }}</h2><StatusBadge :status="job.status" :unit-counts="job.unit_counts" /></div>
          <p>{{ date(job.created) }} 创建 · {{ job.mode === 'auth' ? '账号校验' : '数据任务' }}</p>
        </div>
        <TaskActions :job="job" />
      </div>
      <el-alert v-if="job.error" :title="job.error" type="error" :closable="false" show-icon />
      <el-alert v-else-if="issueMessage" :title="issueMessage"
                :type="(job.unit_counts.leased || job.unit_counts.running) ? 'warning' : 'error'"
                :closable="false" show-icon />
    </el-card>

    <TaskFeedback :processed="job.processed" :unit-counts="job.unit_counts" :total="job.rows"
                  :completed="job.confirmed" :active="count('leased') + count('running')"
                  :success="job.confirmed" :failed="null" :timeout="null"
                  :unconfirmed="count('unknown') + waiting" :pending="count('pending')"
                  :rate="hourlyRate" :elapsed="elapsedSeconds" :capacity="capacityText"
                  :categorized="job.categorized" :status="job.status">
      <template #actions>
        <el-dropdown v-if="job.status === 'completed'" trigger="click" @command="download">
          <el-button type="primary" :icon="Download">导出账号</el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item v-for="item in exportOptions" :key="item.value" :command="item.value">
                {{ item.label }}
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
        <el-button v-else disabled :icon="Download">等待全部确认后可导出</el-button>
      </template>
    </TaskFeedback>

    <ResultList v-if="job.mode === 'auth'" :job="job" />

    <el-collapse v-model="expanded" class="section-card">
      <el-collapse-item title="操作记录" name="events">
        <el-timeline v-if="events.length">
          <el-timeline-item v-for="event in events" :key="event.id" :timestamp="date(event.at)">{{ event.message }}</el-timeline-item>
        </el-timeline>
        <el-empty v-else description="最近记录中没有本任务事件" />
      </el-collapse-item>
    </el-collapse>
  </template>
  <el-empty v-else description="任务不在最近列表中，或当前页面是节点管理页。" />
</template>
