<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { addNodeOpen, api, busy, mutate, refresh, state } from '../composables/useCluster'
const token = ref(''), error = ref(''), name = ref('')
const form = reactive({ processes: 1, threads: 300 })
watch(addNodeOpen, open => { if (open) { token.value = ''; error.value = ''; name.value = '' } })
async function submit() {
  if (!name.value.trim()) { error.value = '请输入节点名称'; return }
  busy.value = true
  try { const result = await api('/api/nodes', { name: name.value, ...form }); token.value = result.token; await refresh() }
  catch (e) { error.value = (e as Error).message }
  finally { busy.value = false }
}
const configOpen = ref(false), node = ref<any>(null), configError = ref('')
const config = reactive({ processes: 1, threads: 300 })
const totalConcurrency = computed(() => Number(config.threads))
function configure(value: any) {
  node.value = value; configError.value = ''
  Object.assign(config, { processes: value.desired_processes, threads: value.desired_threads })
  configOpen.value = true
}
async function save() {
  try { await mutate(`/api/nodes/${node.value.id}/config`, config, '配置已保存，将在后续领取工作包时生效'); configOpen.value = false }
  catch (e) { configError.value = (e as Error).message }
}
function useMaximum() {
  config.processes = 1
  config.threads = Math.min(500, Number(node.value?.max_threads || 500))
}
const deviceOpen = ref(false), deviceNodeId = ref('')
const deviceNode = computed(() => (state.value?.nodes || []).find((item: any) => item.id === deviceNodeId.value))
const devicePool = computed(() => deviceNode.value?.telemetry?.device_pool || {})
const deviceSlots = computed(() => devicePool.value?.slots || [])
const refreshCapacity = computed(() => Math.max(1, Math.min(500,
  Number(deviceNode.value?.effective_threads || deviceNode.value?.desired_threads || deviceNode.value?.max_threads || 1))))
const stateNames: Record<string, string> = { ready: '已注册', registering: '注册中', failed: '失败', pending: '待完成', invalid: '已损坏', missing: '缺失' }
function devices(value: any) { deviceNodeId.value = value.id; deviceOpen.value = true }
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
      { capacity: refreshCapacity.value }, `已按 ${refreshCapacity.value} 设备目标下发刷新`)
  } catch (e) { if (e instanceof Error) ElMessage.error(e.message) }
}
defineExpose({ configure, devices })
</script>
<template>
  <el-dialog v-model="addNodeOpen" :title="token ? '节点已创建' : '添加节点'" width="580px" :close-on-click-modal="false" :show-close="!busy">
    <template v-if="token"><el-alert title="接入凭证仅显示这一次，请妥善保存。" type="warning" :closable="false" show-icon /><p>到节点页面填写总控地址与下方凭证，保存即可连接。</p><el-input :model-value="token" readonly type="textarea" :rows="3" aria-label="新节点接入凭证" /><p class="muted">关闭后不会再次显示凭证。</p></template>
    <el-form v-else label-position="top" @submit.prevent="submit"><el-form-item label="节点名称"><el-input v-model="name" placeholder="例如：工作节点 01" maxlength="80" /></el-form-item><el-form-item label="线程数"><el-input-number v-model="form.threads" :min="1" :max="500" /></el-form-item><p class="muted">节点固定使用 1 个工作进程，默认 300 线程。任务开始先按线程数取满代理；线程复用设备和代理。</p><el-alert v-if="error" :title="error" type="error" :closable="false" /></el-form>
    <template #footer><el-button v-if="token" type="primary" @click="addNodeOpen = false">已保存，关闭</el-button><template v-else><el-button :disabled="busy" @click="addNodeOpen = false">取消</el-button><el-button type="primary" :loading="busy" @click="submit">创建节点</el-button></template></template>
  </el-dialog>
  <el-dialog v-model="configOpen" :title="`${node?.name || ''} · 节点设置`" width="620px" :close-on-click-modal="false" :show-close="!busy">
    <el-form label-position="top"><el-form-item label="线程数"><el-input-number v-model="config.threads" :min="1" :max="Math.min(500, node?.max_threads || 500)" /></el-form-item><p class="muted">固定 1 个工作进程；当前总并发 {{ totalConcurrency }}。代理池目标等于线程数，默认 300；新代理优先使用，可用代理按队列轮转复用。</p><el-button plain @click="useMaximum">使用本机上限</el-button><el-alert v-if="configError" :title="configError" type="error" :closable="false" /></el-form>
    <template #footer><el-button :disabled="busy" @click="configOpen = false">取消</el-button><el-button type="primary" :loading="busy" @click="save">保存配置</el-button></template>
  </el-dialog>
  <el-dialog v-model="deviceOpen" :title="`${deviceNode?.name || ''} · 设备管理`" width="920px" :close-on-click-modal="false" :show-close="!busy">
    <div class="device-summary">
      <span>总设备 <b>{{ devicePool.ready || 0 }}</b></span><span>使用中 <b>{{ devicePool.in_use || 0 }}</b></span><span>注册中 <b>{{ devicePool.registering || 0 }}</b></span>
      <div class="device-actions"><el-button type="danger" plain :loading="busy" @click="clearDevices">一键删除</el-button><el-button type="primary" :loading="busy" @click="refreshDevices">一键刷新（{{ refreshCapacity }} 台）</el-button></div>
    </div>
    <el-table :data="deviceSlots" max-height="520" empty-text="暂无设备，可点击一键刷新">
      <el-table-column prop="slot" label="#" width="58" />
      <el-table-column label="状态" width="88"><template #default="scope">{{ stateNames[scope.row.state] || scope.row.state }}</template></el-table-column>
      <el-table-column prop="label" label="设备" width="118" show-overflow-tooltip />
      <el-table-column prop="model" label="型号" width="160" show-overflow-tooltip />
      <el-table-column prop="proxy" label="注册出口 IP" min-width="180" show-overflow-tooltip />
      <el-table-column prop="used" label="已用账号" width="105" />
      <el-table-column prop="at" label="注册时间" width="165" />
    </el-table>
    <p class="muted">刷新数量等于节点当前生效线程数；同时注册上限为 100，避免设备服务被瞬时压满。</p>
  </el-dialog>
</template>
<style scoped>
.device-summary{display:flex;align-items:center;gap:20px;margin-bottom:16px}.device-summary>span{color:var(--el-text-color-secondary)}.device-summary b{color:var(--el-text-color-primary);font-size:18px}.device-actions{margin-left:auto;display:flex;gap:8px}
</style>
