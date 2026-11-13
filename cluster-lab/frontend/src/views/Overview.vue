<script setup lang="ts">
import { computed, onUnmounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { Plus, ArrowRight, VideoPlay, VideoPause, RefreshRight } from '@element-plus/icons-vue'
import { state, isNode, fmt, createTaskOpen, jobRates, date } from '../composables/useCluster'
import { DEMO_TOTAL, simulatedCounts } from '../lib/simulation.js'
import ConnectPanel from '../components/ConnectPanel.vue'
import TaskActions from '../components/TaskActions.vue'
import TaskFeedback from '../components/TaskFeedback.vue'
const router = useRouter()
const jobs = computed<any[]>(() => state.value?.jobs || [])
const job = computed(() => jobs.value.find(j => j.status === 'running') || jobs.value[0])
const inFlight = computed(() => (job.value?.row_counts?.leased || 0) + (job.value?.row_counts?.running || 0))
const unconfirmed = computed(() => (job.value?.row_counts?.unknown || 0) + Math.max(0, (job.value?.processed || 0) - (job.value?.confirmed || 0)))
const preview = ref(false), playing = ref(false), done = ref(0)
const categories = computed(() => simulatedCounts(done.value))
const demoStatus = computed(() => done.value === DEMO_TOTAL ? 'completed' : playing.value ? 'running' : 'paused')
let timer: ReturnType<typeof setInterval> | undefined
function pauseDemo() { playing.value = false; clearInterval(timer) }
function playDemo() {
  if (done.value === DEMO_TOTAL) done.value = 0
  pauseDemo(); playing.value = true
  timer = setInterval(() => {
    done.value = Math.min(DEMO_TOTAL, done.value + 250)
    if (done.value === DEMO_TOTAL) pauseDemo()
  }, 500)
}
function openDemo() { preview.value = true; done.value = 0; playDemo() }
function closeDemo() { pauseDemo(); preview.value = false }
onUnmounted(pauseDemo)
</script>
<template>
  <ConnectPanel v-if="isNode" />
  <template v-else-if="state">
    <div class="home-task-toolbar"><div><span class="current-task-label">当前任务</span></div><div class="actions"><el-button v-if="!preview" @click="openDemo">查看统计演示</el-button><el-button v-else @click="closeDemo">返回当前任务</el-button><el-button :icon="Plus" type="primary" @click="createTaskOpen = true">上传 TXT 创建任务</el-button></div></div>
    <template v-if="preview"><el-alert title="界面演示：以下数字由浏览器本地生成，不会修改任务，也不向节点下发工作。" type="warning" :closable="false" show-icon /><div class="home-task-heading"><div><h2>离线统计演示 · 10,000 条合成记录</h2><p>模拟成功、失败、超时和跳过的结果反馈。</p></div><div class="actions"><el-button v-if="playing" :icon="VideoPause" @click="pauseDemo">暂停演示</el-button><el-button v-else-if="done < DEMO_TOTAL" type="primary" :icon="VideoPlay" @click="playDemo">继续演示</el-button><el-button :icon="RefreshRight" @click="openDemo">重新演示</el-button></div></div><TaskFeedback :total="DEMO_TOTAL" :completed="done" :active="playing ? Math.min(250, DEMO_TOTAL - done) : 0" :success="categories.success" :failed="categories.failed" :timeout="categories.timeout" :skipped="categories.skipped" :unconfirmed="0" :pending="Math.max(0, DEMO_TOTAL - done - (playing ? 250 : 0))" :rate="playing ? 500 : 0" :status="demoStatus" preview /></template>
    <template v-else-if="job"><div class="home-task-heading"><div><h2>{{ job.name }}</h2><p>{{ date(job.created) }} 创建 · {{ job.mode === 'simulate' ? '离线合成任务' : '文件摘要任务' }}</p></div><TaskActions :job="job" /></div><el-alert v-if="job.error || job.unit_counts.unknown || job.status === 'preparing' || job.status === 'paused'" :title="job.error || (job.unit_counts.unknown ? '存在待确认工作包，请核对后再决定是否重新排队。' : job.status === 'preparing' ? '正在解析文件，当前总数为已解析数量，准备完成后可开始。' : '已暂停派发新工作包，在途工作继续执行。')" :type="job.error ? 'error' : 'info'" :closable="false" show-icon /><TaskFeedback :total="job.rows" :completed="job.confirmed" :active="inFlight" :success="job.confirmed" :failed="null" :timeout="null" :unconfirmed="unconfirmed" :pending="job.row_counts?.pending || 0" :rate="jobRates[job.id] || 0" :status="job.status" /><div class="home-task-bottom"><span>本次任务 · {{ job.unit_counts.completed || 0 }} / {{ job.units }} 个工作包已确认</span><el-button :icon="ArrowRight" @click="router.push('/jobs/' + job.id)">查看任务详情</el-button></div></template>
    <el-card v-else shadow="never"><el-empty description="创建任务后，这里直接展示本次进度与结果。"><el-button type="primary" @click="createTaskOpen = true">创建第一个任务</el-button></el-empty></el-card>
    <div class="home-shortcuts"><el-button link @click="router.push('/jobs')">全部任务</el-button><el-button link @click="router.push('/nodes')">节点配置</el-button><el-button link @click="router.push('/events')">运行日志</el-button></div>
  </template>
</template>
