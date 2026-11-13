<script setup lang="ts">
import { computed } from 'vue'
const props = defineProps<{ status: string }>()
const labels: Record<string, string> = { preparing: '文件准备中', ready: '未入队', queued: '排队中', running: '运行中', paused: '已暂停派发', completed: '已完成', failed: '准备失败', pending: '待分配', leased: '已领取', unknown: '待确认', offline: '离线', idle: '空闲', busy: '运行中', draining: '已暂停接单', confirmed: '已确认', outbox: '待回传', interrupted: '执行中断', rejected: '租约已失效', downloading: '正在下载', stopping: '停止中', interrupted_reported: '已中止' }
const kind = computed(() => ['completed', 'confirmed', 'idle'].includes(props.status) ? 'success' : ['failed', 'interrupted', 'rejected'].includes(props.status) ? 'danger' : ['running', 'busy', 'leased', 'downloading'].includes(props.status) ? 'primary' : props.status === 'offline' ? 'info' : 'warning')
</script>
<template><el-tag :type="kind" effect="light" round><span class="status-dot" />{{ labels[status] || status }}</el-tag></template>
