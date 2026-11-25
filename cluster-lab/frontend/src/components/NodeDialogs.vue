<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import { addNodeOpen, api, busy, mutate, refresh } from '../composables/useCluster'
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
defineExpose({ configure })
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
</template>
