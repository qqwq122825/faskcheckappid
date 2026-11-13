<script setup lang="ts">
import { computed } from 'vue'
import { jobDisplayStatus } from '../lib/jobStatus.js'
const props = defineProps<{ status: string, unitCounts?: Record<string, number> }>()
const displayStatus = computed(() => jobDisplayStatus({ status: props.status, unit_counts: props.unitCounts }))
const labels: Record<string, string> = { preparing: '文件准备中', ready: '未入队', queued: '排队中', running: '运行中', running_with_errors: '运行中 · 部分异常', needs_attention: '异常 · 待确认', paused_with_errors: '已暂停 · 有异常', stopped_with_errors: '已停止 · 有异常', paused: '已暂停派发', stopped: '已停止', completed: '已完成', failed: '准备失败', pending: '待分配', leased: '已领取', unknown: '待确认', offline: '离线', idle: '空闲', busy: '运行中', draining: '已暂停接单', confirmed: '已确认', outbox: '待回传', interrupted: '执行中断', rejected: '租约已失效', downloading: '正在下载', stopping: '停止中', interrupted_reported: '已中止' }
const kind = computed(() => ['completed', 'confirmed', 'idle'].includes(displayStatus.value) ? 'success' : ['failed', 'interrupted', 'rejected', 'needs_attention'].includes(displayStatus.value) ? 'danger' : ['running', 'busy', 'leased', 'downloading'].includes(displayStatus.value) ? 'primary' : displayStatus.value === 'offline' ? 'info' : 'warning')
</script>
<template><el-tag :type="kind" effect="light" round><span class="status-dot" />{{ labels[displayStatus] || displayStatus }}</el-tag></template>
