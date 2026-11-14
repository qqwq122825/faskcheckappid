<script setup lang="ts">
import { reactive, ref, watch } from 'vue'
import { addNodeOpen, api, busy, mutate, refresh } from '../composables/useCluster'
const token = ref(''), error = ref(''), name = ref('')
const form = reactive({ processes: 10, threads: 100 })
watch(addNodeOpen, open => { if (open) { token.value = ''; error.value = ''; name.value = '' } })
async function submit() {
  if (!name.value.trim()) { error.value = '请输入节点名称'; return }
  busy.value = true
  try { const result = await api('/api/nodes', { name: name.value, ...form }); token.value = result.token; await refresh() }
  catch (e) { error.value = (e as Error).message }
  finally { busy.value = false }
}
const configOpen = ref(false), node = ref<any>(null), configError = ref('')
const config = reactive({ processes: 10, threads: 100, enabled: true })
function configure(value: any) {
  node.value = value; configError.value = ''
  Object.assign(config, { processes: value.desired_processes, threads: value.desired_threads, enabled: Boolean(value.enabled) })
  configOpen.value = true
}
async function save() {
  try { await mutate(`/api/nodes/${node.value.id}/config`, config, '配置已保存，将在后续领取工作包时生效'); configOpen.value = false }
  catch (e) { configError.value = (e as Error).message }
}
function useMaximum() {
  config.processes = Number(node.value?.max_processes || 10)
  config.threads = Number(node.value?.max_threads || 100)
}
defineExpose({ configure })
</script>
<template>
  <el-dialog v-model="addNodeOpen" :title="token ? '节点已创建' : '添加节点'" width="580px" :close-on-click-modal="false" :show-close="!busy">
    <template v-if="token"><el-alert title="接入凭证仅显示这一次，请妥善保存。" type="warning" :closable="false" show-icon /><p>到节点页面填写总控地址与下方凭证，保存即可连接。</p><el-input :model-value="token" readonly type="textarea" :rows="3" aria-label="新节点接入凭证" /><p class="muted">关闭后不会再次显示凭证。</p></template>
    <el-form v-else label-position="top" @submit.prevent="submit"><el-form-item label="节点名称"><el-input v-model="name" placeholder="例如：工作节点 01" maxlength="80" /></el-form-item><div class="form-grid"><el-form-item label="工作进程数"><el-input-number v-model="form.processes" :min="1" :max="10" /></el-form-item><el-form-item label="每进程线程数"><el-input-number v-model="form.threads" :min="1" :max="100" /></el-form-item></div><p class="muted">默认直接使用 10 进程 × 100 线程；节点领取新工作包时按这个配置执行。</p><el-alert v-if="error" :title="error" type="error" :closable="false" /></el-form>
    <template #footer><el-button v-if="token" type="primary" @click="addNodeOpen = false">已保存，关闭</el-button><template v-else><el-button :disabled="busy" @click="addNodeOpen = false">取消</el-button><el-button type="primary" :loading="busy" @click="submit">创建节点</el-button></template></template>
  </el-dialog>
  <el-dialog v-model="configOpen" :title="`${node?.name || ''} · 节点设置`" width="620px" :close-on-click-modal="false" :show-close="!busy">
    <el-form label-position="top"><div class="form-grid"><el-form-item label="工作进程数"><el-input-number v-model="config.processes" :min="1" :max="node?.max_processes || 10" /></el-form-item><el-form-item label="每进程线程数"><el-input-number v-model="config.threads" :min="1" :max="node?.max_threads || 100" /></el-form-item></div><p class="muted">本机硬上限 {{ node?.max_processes || '—' }} 进程 × {{ node?.max_threads || '—' }} 线程。节点下次领包时就按保存的进程和线程数执行。</p><el-button plain @click="useMaximum">使用本机上限</el-button><el-form-item><el-switch v-model="config.enabled" aria-label="允许领取新工作包" active-text="允许领取新工作包" /></el-form-item>
    <el-alert v-if="configError" :title="configError" type="error" :closable="false" /></el-form>
    <template #footer><el-button :disabled="busy" @click="configOpen = false">取消</el-button><el-button type="primary" :loading="busy" @click="save">保存配置</el-button></template>
  </el-dialog>
</template>
