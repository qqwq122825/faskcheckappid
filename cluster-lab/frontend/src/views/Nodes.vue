<script setup lang="ts">
import { ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus, Setting, List } from '@element-plus/icons-vue'
import { state, isNode, addNodeOpen, busy, mutate, fmt, metric, bandwidth, date } from '../composables/useCluster'
import StatusBadge from '../components/StatusBadge.vue'
import NodeDialogs from '../components/NodeDialogs.vue'
import ConnectPanel from '../components/ConnectPanel.vue'
import DevicePoolDrawer from '../components/DevicePoolDrawer.vue'
const dialogs = ref<InstanceType<typeof NodeDialogs> | null>(null)
const devices = ref<InstanceType<typeof DevicePoolDrawer> | null>(null)
async function toggle(node: any) {
  try { await mutate(`/api/nodes/${node.id}/config`, { processes: node.desired_processes, threads: node.desired_threads, enabled: !node.enabled }, node.enabled ? '已暂停接单，在途工作继续' : '已恢复接单') }
  catch (e) { ElMessage.error((e as Error).message) }
}
async function stop(node: any) {
  try {
    await ElMessageBox.confirm('停止该节点当前执行的工作并暂停接单。已确认结果保留，未完成工作包进入待确认；其他节点继续运行。', `停止 ${node.name} 当前工作？`, { type: 'warning', confirmButtonText: '停止当前工作', cancelButtonText: '取消' })
    await mutate(`/api/nodes/${node.id}/stop`, {}, '停止指令已发出，等待节点确认')
  } catch (e) { if (e instanceof Error) ElMessage.error(e.message) }
}
</script>
<template><ConnectPanel v-if="isNode" /><template v-else><div class="section-heading page-section-heading"><div><h2>执行节点 <span class="muted">{{ state?.nodes?.length || 0 }}</span></h2><p>整机采样包含其他程序用量；同机多个节点共享宿主机指标。</p></div><el-button type="primary" :icon="Plus" @click="addNodeOpen = true">添加节点</el-button></div><el-empty v-if="!state?.nodes?.length" description="暂无节点，添加后到节点页面连接一次即可。" />
  <el-card v-for="node in state?.nodes || []" :key="node.id" shadow="never" class="node-card"><template #header><div class="section-heading"><div><div class="task-name"><h2>{{ node.name }}</h2><StatusBadge :status="node.status" /></div><p>{{ node.hostname || '等待连接' }} · 最近心跳 {{ date(node.last_seen) }}</p></div><div class="actions"><el-button :icon="List" @click="devices?.show(node)">设备列表</el-button><el-button :icon="Setting" @click="dialogs?.configure(node)">节点设置</el-button><el-button :disabled="busy || node.status === 'stopping'" @click="toggle(node)">{{ node.enabled ? '暂停接单' : '恢复接单' }}</el-button><el-button type="danger" plain :disabled="busy || !node.active || ['offline', 'stopping'].includes(node.status)" @click="stop(node)">停止当前工作</el-button></div></div></template>
    <div class="node-metrics"><div><span>处理速度</span><strong>{{ fmt(node.rate) }} <small>条 / 秒</small></strong></div><div><span>执行进程 / 配额</span><strong>{{ node.telemetry?.worker_processes ?? '—' }} <small>/ {{ node.effective_processes || '—' }}</small></strong></div><div><span>整机 CPU / 内存</span><strong>{{ metric(node.telemetry?.cpu_percent) }} <small>/ {{ metric(node.telemetry?.memory_percent) }}</small></strong></div><div><span>整机上下行</span><strong class="network-value">↑ {{ bandwidth(node.telemetry?.upload_bps) }}<br>↓ {{ bandwidth(node.telemetry?.download_bps) }}</strong></div><div><span>累计已确认</span><strong>{{ fmt(node.confirmed) }} <small>条</small></strong></div></div><div class="node-foot"><span>{{ node.active }} 个包在途 · 期望 {{ node.desired_processes }} × {{ node.desired_threads }} · 生效 {{ node.effective_processes || '—' }} × {{ node.effective_threads || '—' }} · 本机上限 {{ node.max_processes || '—' }} × {{ node.max_threads || '—' }}</span><el-tag type="info">配置对后续工作包生效</el-tag></div><el-alert v-if="node.status === 'stopping' || node.last_error" :title="node.status === 'stopping' ? '正在等待节点中止并回报，当前在途数量尚未清零。' : node.last_error" type="warning" :closable="false" />
  </el-card><NodeDialogs ref="dialogs" /><DevicePoolDrawer ref="devices" /></template></template>
