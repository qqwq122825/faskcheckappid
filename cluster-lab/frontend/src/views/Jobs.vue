<script setup lang="ts">
import { computed, ref } from 'vue'
import { Plus, Search } from '@element-plus/icons-vue'
import { state, isNode, createTaskOpen, fmt, date } from '../composables/useCluster'
import JobTable from '../components/JobTable.vue'
import StatusBadge from '../components/StatusBadge.vue'
const search = ref(''), status = ref('')
const jobs = computed(() => (state.value?.jobs || []).filter((job: any) => (!status.value || job.status === status.value) && job.name.toLowerCase().includes(search.value.toLowerCase())))
</script>
<template><el-card shadow="never" class="section-card"><template #header><div class="section-heading"><div><h2>{{ isNode ? '本机工作包' : '全部任务' }}</h2><p>{{ isNode ? '展示最近 60 个本地工作包。' : '展示最近 50 个任务，进入详情查看本次统计和导出。' }}</p></div><el-button v-if="!isNode" type="primary" :icon="Plus" @click="createTaskOpen = true">上传 TXT 创建任务</el-button></div></template>
  <template v-if="!isNode"><div class="table-toolbar"><el-input v-model="search" clearable :prefix-icon="Search" placeholder="搜索任务名称" aria-label="搜索任务" /><el-select v-model="status" clearable placeholder="全部状态" aria-label="筛选任务状态"><el-option label="运行中" value="running" /><el-option label="待启动" value="ready" /><el-option label="已暂停" value="paused" /><el-option label="已完成" value="completed" /><el-option label="准备失败" value="failed" /></el-select></div><JobTable :jobs="jobs" /></template>
  <el-table v-else :data="state?.tasks || []" stripe empty-text="暂未领取工作包"><el-table-column label="工作包" min-width="180"><template #default="{ row }">工作包 #{{ row.seq }}<div class="muted table-meta">{{ row.job_id.slice(0, 8) }}</div></template></el-table-column><el-table-column label="状态" min-width="160"><template #default="{ row }"><StatusBadge :status="row.status" /><p v-if="row.error" class="error-text">{{ row.error }}</p></template></el-table-column><el-table-column label="执行进度" min-width="180"><template #default="{ row }">{{ fmt(row.processed) }} / {{ fmt(row.rows) }}</template></el-table-column><el-table-column label="更新时间" min-width="200"><template #default="{ row }">{{ date(row.updated) }}</template></el-table-column></el-table>
</el-card></template>
