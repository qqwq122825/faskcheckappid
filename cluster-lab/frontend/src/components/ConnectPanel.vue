<script setup lang="ts">
import { reactive, ref, watch } from 'vue'
import { state, mutate, busy, date } from '../composables/useCluster'
import MetricCard from './MetricCard.vue'
const form = reactive({ url: '', token: '' }), initialized = ref(false), error = ref('')
watch(state, value => { if (value && !initialized.value) { form.url = value.config.url || ''; initialized.value = true } }, { immediate: true })
async function save() {
  error.value = ''
  try { await mutate('/api/connect', form, '已保存，节点正在连接总控'); form.token = '' }
  catch (e) { error.value = (e as Error).message }
}
</script>
<template><template v-if="state"><div class="metrics-grid node-summary"><MetricCard label="总控心跳" :value="state.connected ? '已连接' : '未连接'" :note="`最近成功：${date(state.last_success)}`" :accent="state.connected" /><MetricCard label="本机执行进程" :value="`${state.active_processes} / ${state.max_processes}`" :note="`线程硬上限 ${state.max_threads}`" /><MetricCard label="本机已确认" :value="state.confirmed" unit="条" note="已收到总控确认" /></div><el-card shadow="never" class="section-card connection-card"><template #header><h2>连接总控</h2><p class="muted">部署时连接一次，日常操作在总控完成。</p></template><el-form label-position="top" @submit.prevent="save"><el-form-item label="总控地址"><el-input v-model="form.url" placeholder="https://controller.example.com" type="url" /></el-form-item><el-form-item :label="state.config.token_saved ? '节点接入凭证（已保存，留空沿用）' : '节点接入凭证'"><el-input v-model="form.token" type="password" show-password autocomplete="off" placeholder="粘贴总控创建节点时生成的凭证" /></el-form-item><el-alert v-if="error || state.error" :title="error || state.error" type="error" :closable="false" /><el-button type="primary" :loading="busy" native-type="submit">保存并连接</el-button></el-form><el-divider /><el-descriptions :column="1" border><el-descriptions-item label="主机名">{{ state.hostname }}</el-descriptions-item><el-descriptions-item label="生效配置">{{ state.settings.processes || 0 }} 进程 × {{ state.settings.threads || 0 }} 线程</el-descriptions-item><el-descriptions-item label="待回传记录">{{ state.outbox }} 条</el-descriptions-item></el-descriptions></el-card></template></template>
