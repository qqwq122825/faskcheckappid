<script setup lang="ts">
import { computed } from 'vue'
import { useRouter } from 'vue-router'
import { ArrowRight } from '@element-plus/icons-vue'
import { state, isNode, fmt, percent, date } from '../composables/useCluster'
import ConnectPanel from '../components/ConnectPanel.vue'
import MetricCard from '../components/MetricCard.vue'
import StatusBadge from '../components/StatusBadge.vue'
const router = useRouter()
const jobs = computed<any[]>(() => state.value?.jobs || [])
const job = computed(() => jobs.value.find(j => j.id === state.value?.current_job_id) || jobs.value[0])
const totals = computed(() => state.value?.totals || {})
const queue = computed<any[]>(() => state.value?.queue || [])
</script>
<template>
  <ConnectPanel v-if="isNode" />
  <template v-else-if="state">
    <div class="metrics-grid cluster-overview">
      <MetricCard label="节点总数" :value="fmt(totals.nodes)" unit="台" note="已登记的执行节点" />
      <MetricCard label="在线节点" :value="fmt(totals.online)" unit="台" note="心跳连接正常" accent />
      <MetricCard label="工作节点" :value="fmt(totals.busy)" unit="台" note="当前有在途分片" />
      <MetricCard label="排队任务" :value="fmt(totals.queued)" unit="个" note="包含已入队且正在准备文件的任务" />
    </div>
    <div class="home-task-toolbar"><span class="current-task-label">{{ state.current_job_id ? '当前任务' : job?.status === 'queued' || job?.status === 'preparing' ? '等待执行' : '最近任务' }}</span><el-button :icon="ArrowRight" @click="router.push('/jobs')">任务管理</el-button></div>
    <el-card v-if="job" shadow="never" class="section-card">
      <div class="section-heading"><div><div class="task-name"><h2>{{ job.name }}</h2><StatusBadge :status="job.status" :unit-counts="job.unit_counts" /></div><p>{{ date(job.created) }} 创建 · {{ fmt(Math.max(job.processed || 0, job.confirmed || 0)) }} 条已处理 · {{ fmt(job.confirmed) }} 条已确认</p></div><el-button type="primary" :icon="ArrowRight" @click="router.push('/jobs/' + job.id)">查看任务详情</el-button></div>
      <el-progress class="summary-progress" :percentage="Number(percent(job).toFixed(1))" :stroke-width="10" />
      <p v-if="job.status === 'paused'" class="muted">当前任务暂停中，后续任务保留排队顺序。</p>
    </el-card>
    <el-card v-else shadow="never" class="section-card"><el-empty description="暂无任务，请到任务管理上传 TXT 并加入队列。"><el-button type="primary" @click="router.push('/jobs')">前往任务管理</el-button></el-empty></el-card>
    <el-card shadow="never" class="section-card"><template #header><div class="section-heading"><div><h2>等待队列 <span class="muted">{{ fmt(totals.queued) }} 个任务</span></h2><p>按加入顺序执行，当前任务全部确认后自动接续。显示接下来的 10 个任务。</p></div><el-button link @click="router.push('/jobs')">查看全部任务</el-button></div></template><el-table :data="queue" empty-text="暂无排队任务"><el-table-column prop="position" label="顺序" width="85" /><el-table-column label="任务名称" min-width="240"><template #default="{ row }"><el-button link class="job-link" @click="router.push('/jobs/' + row.id)">{{ row.name }}</el-button></template></el-table-column><el-table-column label="记录数" width="150"><template #default="{ row }">{{ fmt(row.rows) }}</template></el-table-column><el-table-column label="状态" width="155"><template #default="{ row }"><StatusBadge :status="row.status" /></template></el-table-column></el-table></el-card>
  </template>
</template>
