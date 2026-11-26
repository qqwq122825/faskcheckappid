<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { addNodeOpen, api, busy, mutate, refresh, state } from '../composables/useCluster'
const token = ref(''), error = ref(''), name = ref('')
const form = reactive({ processes: 2, threads: 250 })
watch(addNodeOpen, open => { if (open) { token.value = ''; error.value = ''; name.value = '' } })
async function submit() {
  if (!name.value.trim()) { error.value = '请输入节点名称'; return }
  busy.value = true
  try { const result = await api('/api/nodes', { name: name.value, ...form }); token.value = result.token; await refresh() }
  catch (e) { error.value = (e as Error).message }
  finally { busy.value = false }
}
const configOpen = ref(false), node = ref<any>(null), configError = ref('')
const config = reactive({ processes: 2, threads: 250 })
const totalConcurrency = computed(() => Number(config.processes) * Number(config.threads))
function configure(value: any) {
  node.value = value; configError.value = ''
  Object.assign(config, { processes: value.desired_processes, threads: value.desired_threads })
  configOpen.value = true
}
async function save() {
  try { await mutate(`/api/nodes/${node.value.id}/config`, config, '配置已保存，将在后续领取工作包时生效'); configOpen.value = false }
  catch (e) { configError.value = (e as Error).message }
}
const deviceOpen = ref(false), deviceNodeId = ref(''), verifyExitIp = ref(false)
const deviceNode = computed(() => (state.value?.nodes || []).find((item: any) => item.id === deviceNodeId.value))
const devicePool = computed(() => deviceNode.value?.telemetry?.device_pool || {})
const deviceSlots = computed(() => devicePool.value?.slots || [])
const refreshCapacity = computed(() => Math.max(1,
  Number(deviceNode.value?.effective_processes || deviceNode.value?.desired_processes || 1) *
  Number(deviceNode.value?.effective_threads || deviceNode.value?.desired_threads || 1)))
const stateNames: Record<string, string> = { ready: '已注册', registering: '注册中', failed: '失败', pending: '待完成', invalid: '已损坏', missing: '缺失' }
function devices(value: any) { deviceNodeId.value = value.id; verifyExitIp.value = false; deviceOpen.value = true }
async function clearDevices() {
  if (!deviceNode.value) return
  try {
    await ElMessageBox.confirm('将删除该节点全部设备身份、注册记录和使用计数。', '一键删除全部设备？',
      { type: 'warning', confirmButtonText: '删除全部', cancelButtonText: '取消' })
    busy.value = true
    await api(`/api/nodes/${deviceNode.value.id}/devices`, {}, 'DELETE')
    await refresh(); ElMessage.success('清空指令已下发')
  } catch (e) { if (e instanceof Error) ElMessage.error(e.message) }
  finally { busy.value = false }
}
async function refreshDevices() {
  if (!deviceNode.value) return
  try {
    await ElMessageBox.confirm(
      `将清空旧设备，然后按节点当前 ${refreshCapacity.value} 线程目标重建。`,
      '一键刷新设备？', { type: 'warning', confirmButtonText: '开始刷新', cancelButtonText: '取消' })
    await mutate(`/api/nodes/${deviceNode.value.id}/devices/refresh`,
      { capacity: refreshCapacity.value, verify_exit_ip: verifyExitIp.value },
      `已按 ${refreshCapacity.value} 设备目标下发刷新`)
  } catch (e) { if (e instanceof Error) ElMessage.error(e.message) }
}
async function fillDevices() {
  if (!deviceNode.value) return
  try {
    await mutate(`/api/nodes/${deviceNode.value.id}/devices`,
      { capacity: refreshCapacity.value, verify_exit_ip: verifyExitIp.value },
      `已按 ${refreshCapacity.value} 台目标补齐；现有设备不会删除`)
  } catch (e) { if (e instanceof Error) ElMessage.error(e.message) }
}
defineExpose({ configure, devices })
</script>
<template>
  <el-dialog v-model="addNodeOpen" :title="token ? '节点已创建' : '添加节点'" width="580px" :close-on-click-modal="false" :show-close="!busy">
    <template v-if="token"><el-alert title="接入凭证仅显示这一次，请妥善保存。" type="warning" :closable="false" show-icon /><p>到节点页面填写总控地址与下方凭证，保存即可连接。</p><el-input :model-value="token" readonly type="textarea" :rows="3" aria-label="新节点接入凭证" /><p class="muted">关闭后不会再次显示凭证。</p></template>
    <el-form v-else label-position="top" @submit.prevent="submit"><el-form-item label="节点名称"><el-input v-model="name" placeholder="例如：工作节点 01" maxlength="80" /></el-form-item><el-form-item label="进程数"><el-input-number v-model="form.processes" :min="1" /></el-form-item><el-form-item label="每进程线程数"><el-input-number v-model="form.threads" :min="1" /></el-form-item><p class="muted">默认 2 个工作进程、每进程 250 线程；节点不设软件并发上限，保存多少就按多少执行。设备文件在任务启动时平均分给各进程，每个进程独立维护自己的 IP 池和设备集合。</p><el-alert v-if="error" :title="error" type="error" :closable="false" /></el-form>
    <template #footer><el-button v-if="token" type="primary" @click="addNodeOpen = false">已保存，关闭</el-button><template v-else><el-button :disabled="busy" @click="addNodeOpen = false">取消</el-button><el-button type="primary" :loading="busy" @click="submit">创建节点</el-button></template></template>
  </el-dialog>
  <el-dialog v-model="configOpen" class="node-config-dialog" width="min(860px, calc(100vw - 32px))" :close-on-click-modal="false" :show-close="!busy">
    <template #header><h2 class="node-config-title">{{ node?.name || '' }} · 节点设置</h2></template>
    <el-form class="node-config-form" label-position="top">
      <div class="node-config-controls">
        <el-form-item label="进程数"><el-input-number v-model="config.processes" class="node-process-control" :min="1" /></el-form-item>
        <el-form-item label="每进程线程数"><el-input-number v-model="config.threads" class="node-thread-control" :min="1" /></el-form-item>
      </div>
      <p class="node-config-help">当前 {{ config.processes }} 个工作进程 × 每进程 {{ config.threads }} 线程，总并发 {{ totalConcurrency }}。节点不设软件并发上限，保存多少就按多少执行；实际承载能力仍取决于机器资源。每个进程独立维护自己的 IP 池与设备集合；线程只从本进程集合借出使用次数最少的可用设备。启动时先完成设备注册并取得公共头，再领取账号。</p>
      <el-alert v-if="configError" :title="configError" type="error" :closable="false" />
    </el-form>
    <template #footer><div class="node-config-footer"><el-button :disabled="busy" @click="configOpen = false">取消</el-button><el-button type="primary" :loading="busy" @click="save">保存配置</el-button></div></template>
  </el-dialog>
  <el-dialog v-model="deviceOpen" :title="`${deviceNode?.name || ''} · 设备管理`" width="920px" :close-on-click-modal="false" :show-close="!busy">
    <div class="device-summary">
      <span>总设备 <b>{{ devicePool.ready || 0 }}</b></span><span>使用中 <b>{{ devicePool.in_use || 0 }}</b></span><span>注册中 <b>{{ devicePool.registering || 0 }}</b></span>
      <div class="device-actions"><el-button type="danger" plain :loading="busy" @click="clearDevices">一键删除</el-button><el-button plain :loading="busy" @click="refreshDevices">清空并重建</el-button><el-button type="primary" :loading="busy" @click="fillDevices">补齐到 {{ refreshCapacity }} 台</el-button></div>
    </div>
    <div class="device-probe-option"><div><strong>注册后回查出口 IP</strong><p>关闭时不发起额外回查请求，注册速度更快；任务启动时线程自动注册固定跳过回查。</p></div><el-switch v-model="verifyExitIp" /></div>
    <el-table :data="deviceSlots" max-height="520" empty-text="暂无设备，可点击一键刷新">
      <el-table-column prop="slot" label="#" width="58" />
      <el-table-column label="状态" width="88"><template #default="scope">{{ stateNames[scope.row.state] || scope.row.state }}</template></el-table-column>
      <el-table-column prop="label" label="设备" width="118" show-overflow-tooltip />
      <el-table-column prop="model" label="型号" width="160" show-overflow-tooltip />
      <el-table-column prop="proxy" label="注册出口 IP" min-width="180" show-overflow-tooltip />
      <el-table-column prop="used" label="已用账号" width="105" />
      <el-table-column prop="at" label="注册时间" width="165" />
    </el-table>
    <p class="muted">补齐数量等于节点当前生效总并发，只注册缺失槽位并保留现有设备；同时注册上限为 100，避免设备服务被瞬时压满。</p>
  </el-dialog>
</template>
<style scoped>
.device-summary{display:flex;align-items:center;gap:20px;margin-bottom:16px}.device-summary>span{color:var(--el-text-color-secondary)}.device-summary b{color:var(--el-text-color-primary);font-size:18px}.device-actions{margin-left:auto;display:flex;gap:8px}
.device-probe-option{display:flex;align-items:center;justify-content:space-between;gap:24px;margin:-2px 0 16px;padding:12px 14px;border:1px solid var(--el-border-color-lighter);border-radius:10px;background:var(--el-fill-color-light)}.device-probe-option strong{color:var(--el-text-color-primary)}.device-probe-option p{margin:4px 0 0;color:var(--el-text-color-secondary);font-size:13px}
.node-config-title{margin:0;color:#303b45;font-size:28px;font-weight:750;line-height:1.3;letter-spacing:-.02em}
.node-config-form{min-height:300px;padding:22px 18px 8px}
.node-config-form :deep(.el-form-item__label){padding-bottom:14px;color:#485661;font-size:18px;font-weight:650;line-height:1.4}
.node-config-controls{display:grid;grid-template-columns:repeat(2,minmax(0,288px));gap:28px}
.node-process-control,.node-thread-control{width:288px}
.node-process-control :deep(.el-input__wrapper),.node-thread-control :deep(.el-input__wrapper){height:62px;padding:0 62px;background:#fff;box-shadow:0 0 0 1px #d8e0e6 inset}
.node-process-control :deep(.el-input__inner),.node-thread-control :deep(.el-input__inner){color:#44515c;font-size:21px}
.node-process-control :deep(.el-input-number__decrease),.node-process-control :deep(.el-input-number__increase),.node-thread-control :deep(.el-input-number__decrease),.node-thread-control :deep(.el-input-number__increase){width:62px;color:#4e5d68;font-size:22px;background:#f7f9fa;border-color:#d8e0e6}
.node-config-help{max-width:760px;margin:30px 0 20px;color:#7a8793;font-size:17px;font-weight:550;line-height:1.75}
.node-limit-button{height:54px;padding:0 28px;color:#4b5964;font-size:18px;font-weight:650}
.node-config-footer{display:flex;justify-content:flex-end;gap:14px}
.node-config-footer :deep(.el-button){min-width:114px;height:56px;padding:0 26px;font-size:18px;font-weight:700}
:global(.node-config-dialog.el-dialog){overflow:hidden;border-radius:16px;box-shadow:0 24px 70px rgba(26,35,42,.24)}
:global(.node-config-dialog .el-dialog__header){margin:0;padding:36px 42px 8px}
:global(.node-config-dialog .el-dialog__headerbtn){top:21px;right:24px;width:40px;height:40px;font-size:22px}
:global(.node-config-dialog .el-dialog__body){padding:0 42px 8px}
:global(.node-config-dialog .el-dialog__footer){padding:10px 42px 34px}
@media(max-width:640px){.node-config-title{font-size:22px}.node-config-form{min-height:260px;padding:14px 0 4px}.node-config-controls{grid-template-columns:1fr;gap:8px}.node-process-control,.node-thread-control{width:100%}.node-config-help{font-size:15px;margin-top:22px}:global(.node-config-dialog .el-dialog__header){padding:26px 24px 8px}:global(.node-config-dialog .el-dialog__body){padding:0 24px 6px}:global(.node-config-dialog .el-dialog__footer){padding:8px 24px 24px}.node-config-footer :deep(.el-button){min-width:96px;height:50px}}
</style>
