<script setup lang="ts">
import { computed, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ArrowLeft, Download } from '@element-plus/icons-vue'
import { state, fmt, percent, date, jobRates, isNode } from '../composables/useCluster'
import MetricCard from '../components/MetricCard.vue'
import StatusBadge from '../components/StatusBadge.vue'
import TaskActions from '../components/TaskActions.vue'
const route = useRoute(), router = useRouter(), expanded = ref<string[]>([])
const job = computed(() => state.value?.jobs?.find((j: any) => j.id === route.params.id))
const count = (key: string) => job.value?.row_counts?.[key] || 0
const processing = computed(() => Math.max(0, count('leased') + count('running') - Math.max(0, (job.value?.processed || 0) - (job.value?.confirmed || 0))))
const waiting = computed(() => Math.max(0, (job.value?.processed || 0) - (job.value?.confirmed || 0)))
const events = computed(() => state.value?.events?.filter((e: any) => e.job_id === route.params.id) || [])

</script>
<template>
  <el-button :icon="ArrowLeft" class="back-button" @click="router.push('/jobs')">返回任务管理</el-button>
  <template v-if="job && !isNode"><el-card shadow="never" class="section-card task-heading"><div class="section-heading"><div><div class="task-name"><h2>{{ job.name }}</h2><StatusBadge :status="job.status" /></div><p>{{ date(job.created) }} 创建 · {{ job.mode === 'simulate' ? '合成离线演示' : '逐行 SHA-256 摘要' }}</p></div><TaskActions :job="job" /></div><div class="task-progress"><span>整体确认进度</span><strong>{{ percent(job).toFixed(1) }}%</strong></div><el-progress :percentage="Number(percent(job).toFixed(1))" :show-text="false" :stroke-width="14" /><p class="muted">{{ fmt(job.confirmed) }} / {{ fmt(job.rows) }} 条已确认 · {{ job.unit_counts.completed || 0 }} / {{ job.units }} 个工作包完成</p></el-card>
    <el-alert v-if="job.error" :title="job.error" type="error" :closable="false" show-icon />
    <el-alert v-else-if="job.unit_counts.unknown" title="存在待确认工作包，未自动重新执行。请核对后再决定是否重新排队。" type="warning" :closable="false" show-icon />
    <el-alert v-else-if="job.status === 'paused'" title="已暂停派发新工作包，在途工作会继续执行并回传。" type="info" :closable="false" show-icon />
    <el-alert v-else-if="job.status === 'preparing'" title="正在准备文件，当前总数为已解析数量。准备完成后才能开始。" type="info" :closable="false" show-icon />
    <div class="section-label"><h2>进度与结果</h2><span class="muted">仅统计本次任务</span></div>
    <div class="metrics-grid task-metrics"><MetricCard label="总数" :value="fmt(job.rows)" unit="条" :note="job.status === 'preparing' ? '文件仍在解析' : '本次任务记录总量'" /><MetricCard label="已确认" :value="fmt(job.confirmed)" unit="条" note="结果已回传并通过校验" accent /><MetricCard label="剩余" :value="fmt(Math.max(0, job.rows - job.confirmed))" unit="条" note="包含待分配、在途及待确认" /><MetricCard label="本次处理速度" :value="fmt(jobRates[job.id])" unit="条 / 秒" note="按本页观测的执行增量估算" /><MetricCard label="待分配" :value="fmt(count('pending'))" unit="条" note="尚未领取的记录" /><MetricCard label="在途未执行" :value="fmt(processing)" unit="条" note="已领取包内尚未执行的记录" /><MetricCard label="已执行待确认" :value="fmt(waiting)" unit="条" note="仍需回传并校验完整结果" /><MetricCard label="异常待确认" :value="fmt(count('unknown'))" unit="条" note="中断或租约过期的工作包" /></div>
    <el-card shadow="never" class="section-card"><template #header><div class="section-heading"><div><h2>本次任务导出</h2><p>全部工作包确认后，按上传顺序导出 TXT，每行一条原始记录。</p></div><el-button v-if="job.status === 'completed'" tag="a" :href="`/api/jobs/${job.id}/results`" :icon="Download" type="primary">下载 TXT 结果</el-button><el-button v-else disabled :icon="Download">等待全部确认</el-button></div></template><el-descriptions :column="2" border><el-descriptions-item label="结果内容">TXT 原始记录，保持上传顺序</el-descriptions-item><el-descriptions-item label="文件命名">日期_处理成功_条数.txt（北京时间）</el-descriptions-item><el-descriptions-item label="完整结果">{{ job.status === 'completed' ? '可下载' : '等待全部确认' }}</el-descriptions-item><el-descriptions-item label="任务编号">{{ job.id }}</el-descriptions-item><el-descriptions-item label="工作包数">{{ job.units }}</el-descriptions-item></el-descriptions></el-card>
    <el-collapse v-model="expanded"><el-collapse-item title="查看本次任务的最近操作记录" name="events"><el-timeline v-if="events.length"><el-timeline-item v-for="event in events" :key="event.id" :timestamp="date(event.at)">{{ event.message }}</el-timeline-item></el-timeline><el-empty v-else description="最近记录中没有本任务事件" /></el-collapse-item></el-collapse>
  </template>
  <el-empty v-else description="任务不在最近列表中，或当前页面是节点管理页。" />
</template>
