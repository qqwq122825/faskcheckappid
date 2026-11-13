<script setup lang="ts">
import { useRouter } from 'vue-router'
import { fmt, percent, date } from '../composables/useCluster'
import StatusBadge from './StatusBadge.vue'
defineProps<{ jobs: any[] }>()
const router = useRouter()
</script>
<template>
  <el-table :data="jobs" stripe class="job-table" empty-text="暂无任务，点击右上角创建任务">
    <el-table-column label="任务名称" min-width="250"><template #default="{ row }"><el-button link type="primary" class="job-link" @click="router.push('/jobs/' + row.id)">{{ row.name }}</el-button><div class="muted table-meta">{{ date(row.created) }} · {{ row.mode === 'simulate' ? '离线演示' : '文件摘要' }}</div></template></el-table-column>
    <el-table-column label="状态" width="145"><template #default="{ row }"><StatusBadge :status="row.status" /></template></el-table-column>
    <el-table-column label="任务进度" min-width="220"><template #default="{ row }"><div class="progress-label">{{ fmt(row.confirmed) }} / {{ fmt(row.rows) }} 条</div><el-progress :percentage="Number(percent(row).toFixed(1))" :stroke-width="7" /></template></el-table-column>
    <el-table-column label="操作" width="130" fixed="right"><template #default="{ row }"><el-button plain @click="router.push('/jobs/' + row.id)">查看详情</el-button></template></el-table-column>
  </el-table>
</template>
