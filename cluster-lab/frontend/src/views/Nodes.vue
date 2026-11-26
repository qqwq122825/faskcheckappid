<script setup lang="ts">
import { ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus, Setting, Refresh } from '@element-plus/icons-vue'
import { state, isNode, addNodeOpen, busy, mutate, fmt, metric, bandwidth, date } from '../composables/useCluster'
import StatusBadge from '../components/StatusBadge.vue'
import NodeDialogs from '../components/NodeDialogs.vue'
import ConnectPanel from '../components/ConnectPanel.vue'
const dialogs = ref<InstanceType<typeof NodeDialogs> | null>(null)
const updateOpen = ref(false)
const updateRef = ref('main')
const updateParallel = ref(2)
const updateNodes = ref<string[]>([])
const updateLabels: Record<string, string> = {
  queued: '等待排空', dispatched: '已下发', scheduled: '已接收', fetching: '拉取代码',
  installing: '安装依赖', restarting: '重启中', verifying: '健康检查',
  success: '更新成功', failed: '更新失败', rolled_back: '已自动回滚'
}
function showUpdate() {
  updateNodes.value = (state.value?.nodes || []).filter((node: any) => node.status !== 'offline').map((node: any) => node.id)
  updateOpen.value = true
}
async function submitUpdate() {
  if (!updateRef.value.trim()) return ElMessage.warning('请填写 commit、tag 或分支')
  if (!updateNodes.value.length) return ElMessage.warning('请至少选择一个在线节点')
  try {
    await ElMessageBox.confirm(
      '选中节点会先暂停接新包，等待在途工作完成后分批更新；更新失败会自动回滚并保持暂停。',
      `滚动更新 ${updateNodes.value.length} 个节点？`,
      { type: 'warning', confirmButtonText: '开始滚动更新', cancelButtonText: '取消' }
    )
    await mutate('/api/nodes/update', { node_ids: updateNodes.value, ref: updateRef.value.trim(), parallel: updateParallel.value }, '滚动更新已加入队列')
    updateOpen.value = false
  } catch (e) { if (e instanceof Error) ElMessage.error(e.message) }
}
async function toggle(node: any) {
  try { await mutate(`/api/nodes/${node.id}/config`, { processes: node.desired_processes, threads: node.desired_threads, enabled: !node.enabled }, node.enabled ? '已暂停接单，在途工作继续' : '已开始接单') }
  catch (e) { ElMessage.error((e as Error).message) }
}
async function stop(node: any) {
  try {
    await ElMessageBox.confirm('停止该节点当前执行的工作并暂停接单。已确认结果保留，未完成工作包进入待确认；其他节点继续运行。', `停止 ${node.name} 当前工作？`, { type: 'warning', confirmButtonText: '停止当前工作', cancelButtonText: '取消' })
    await mutate(`/api/nodes/${node.id}/stop`, {}, '停止指令已发出，等待节点确认')
  } catch (e) { if (e instanceof Error) ElMessage.error(e.message) }
}
async function checkProxy(node: any) {
  try { await mutate(`/api/nodes/${node.id}/proxy-check`, {}, '代理校验已下发，等待节点回报') }
  catch (e) { ElMessage.error((e as Error).message) }
}
function proxyCheckTitle(node: any) {
  const check = node.telemetry?.proxy_check
  if (!check) return ''
  if (check.status === 'checking') return '正在提取一条新代理并检测出口地址与 Google…'
  const timing = [check.ip_ms == null ? '' : `出口 ${check.ip_ms}ms`, check.google_ms == null ? '' : `Google ${check.google_ms}ms`].filter(Boolean).join(' · ')
  if (check.status === 'ok') return `代理正常 · 出口 ${check.exit_ip} · Google HTTP ${check.google_status}${timing ? ` · ${timing}` : ''}`
  return `代理异常（${check.stage || 'unknown'}）· ${check.message || '检测失败'}${check.exit_ip ? ` · 出口 ${check.exit_ip}` : ''}`
}
</script>
<template><ConnectPanel v-if="isNode" /><template v-else><div class="section-heading page-section-heading"><div><h2>执行节点 <span class="muted">{{ state?.nodes?.length || 0 }}</span></h2><p>整机采样包含其他程序用量；同机多个节点共享宿主机指标。</p></div><div class="actions"><el-button :icon="Refresh" :disabled="busy || !state?.nodes?.length" @click="showUpdate">滚动更新</el-button><el-button type="primary" :icon="Plus" @click="addNodeOpen = true">添加节点</el-button></div></div><el-empty v-if="!state?.nodes?.length" description="暂无节点，添加后到节点页面连接一次即可。" />
  <el-card v-for="node in state?.nodes || []" :key="node.id" shadow="never" class="node-card"><template #header><div class="section-heading"><div><div class="task-name"><h2>{{ node.name }}</h2><StatusBadge :status="node.status" /></div><p>{{ node.hostname || '等待连接' }} · 最近心跳 {{ date(node.last_seen) }}</p></div><div class="actions"><el-button @click="dialogs?.devices(node)">设备管理</el-button><el-button :loading="node.telemetry?.proxy_check?.status === 'checking'" :disabled="busy || node.status === 'offline'" @click="checkProxy(node)">校验代理状态</el-button><el-button :icon="Setting" @click="dialogs?.configure(node)">节点设置</el-button><el-button :disabled="busy || node.status === 'stopping'" @click="toggle(node)">{{ node.enabled ? '暂停接单' : '开始接单' }}</el-button><el-button type="danger" plain :disabled="busy || !node.active || ['offline', 'stopping'].includes(node.status)" @click="stop(node)">停止当前工作</el-button></div></div></template>
    <div class="node-metrics"><div><span>处理速度</span><strong>{{ fmt(node.rate) }} <small>条 / 秒</small></strong></div><div><span>执行进程 / 配置</span><strong>{{ node.telemetry?.worker_processes ?? '—' }} <small>/ {{ node.effective_processes || '—' }}</small></strong></div><div><span>整机 CPU / 内存</span><strong>{{ metric(node.telemetry?.cpu_percent) }} <small>/ {{ metric(node.telemetry?.memory_percent) }}</small></strong></div><div><span>整机上下行</span><strong class="network-value">↑ {{ bandwidth(node.telemetry?.upload_bps) }}<br>↓ {{ bandwidth(node.telemetry?.download_bps) }}</strong></div><div><span>累计已确认</span><strong>{{ fmt(node.confirmed) }} <small>条</small></strong></div></div><div class="node-foot"><span>{{ node.active }} 个包在途 · 设置并生效 {{ node.effective_processes || node.desired_processes }} × {{ node.effective_threads || node.desired_threads }} · 无软件并发上限 · 版本 {{ node.version || '—' }} · Python {{ node.telemetry?.python_version || '—' }} · gmpy2 {{ node.telemetry?.gmpy2_version || '—' }} · SRP {{ node.telemetry?.srp_math_backend || '—' }} · 设备 {{ fmt(node.telemetry?.device_pool?.ready) }} · 使用中 {{ fmt(node.telemetry?.device_pool?.in_use) }} · 注册中 {{ fmt(node.telemetry?.device_pool?.registering) }}</span><el-tag v-if="node.update_state" :type="['failed', 'rolled_back'].includes(node.update_state) ? 'danger' : node.update_state === 'success' ? 'success' : 'warning'">{{ updateLabels[node.update_state] || node.update_state }}</el-tag><el-tag v-else type="info">每进程独立 IP 池与设备</el-tag></div><el-alert v-if="node.update_message && node.update_state && node.update_state !== 'success'" :title="`${node.update_ref || ''} · ${node.update_message}`" :type="['failed', 'rolled_back'].includes(node.update_state) ? 'error' : 'info'" :closable="false" /><el-alert v-if="node.status === 'stopping' || node.last_error" :title="node.status === 'stopping' ? '正在等待节点中止并回报，当前在途数量尚未清零。' : node.last_error" type="warning" :closable="false" />
    <el-alert v-if="node.telemetry?.proxy_check" :title="proxyCheckTitle(node)" :type="node.telemetry.proxy_check.status === 'ok' ? 'success' : node.telemetry.proxy_check.status === 'failed' ? 'error' : 'info'" :closable="false" show-icon />
  </el-card>
  <el-dialog v-model="updateOpen" title="滚动更新执行节点" width="620px" :close-on-click-modal="!busy" :show-close="!busy">
    <el-form label-position="top">
      <el-form-item label="目标 Git commit、tag 或分支"><el-input v-model="updateRef" placeholder="建议填写本次推送的完整 commit SHA" maxlength="120" /></el-form-item>
      <el-form-item label="同时更新节点数"><el-input-number v-model="updateParallel" :min="1" :max="5" :precision="0" /><span class="field-note">建议 1–2 台，始终保留其他节点在线。</span></el-form-item>
      <el-form-item label="更新节点">
        <el-checkbox-group v-model="updateNodes">
          <el-checkbox v-for="node in state?.nodes || []" :key="node.id" :value="node.id" :disabled="node.status === 'offline'">{{ node.name }}（{{ node.version || '未知版本' }}{{ node.status === 'offline' ? '，离线' : '' }}）</el-checkbox>
        </el-checkbox-group>
      </el-form-item>
      <el-alert title="流程：暂停接单 → 等待在途包结束 → git fetch/checkout → 安装依赖 → systemd 重启 → 健康检查；失败自动回滚并保持暂停。" type="info" :closable="false" show-icon />
    </el-form>
    <template #footer><el-button :disabled="busy" @click="updateOpen = false">取消</el-button><el-button type="primary" :loading="busy" @click="submitUpdate">开始滚动更新</el-button></template>
  </el-dialog>
  <NodeDialogs ref="dialogs" /></template></template>
