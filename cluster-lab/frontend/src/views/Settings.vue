<script setup lang="ts">
import { ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { session, isNode, state, busy, mutate } from '../composables/useCluster'
const maxUnitRows = ref(10000), error = ref('')
watch(() => state.value?.settings?.max_unit_rows, value => {
  if (value != null) maxUnitRows.value = value
}, { immediate: true })
async function save() {
  error.value = ''
  if (!Number.isInteger(maxUnitRows.value) || maxUnitRows.value < 50 || maxUnitRows.value > 10000) {
    error.value = '单个分片最大条数应为 50–10,000 的整数'; return
  }
  try { await mutate('/api/settings', { max_unit_rows: maxUnitRows.value }, '分片上限已保存，仅对新创建的任务生效') }
  catch (e) { error.value = (e as Error).message; ElMessage.error(error.value) }
}
</script>
<template>
  <el-card v-if="!isNode" shadow="never" class="section-card">
    <template #header><h2>任务分片设置</h2></template>
    <el-form label-position="top" @submit.prevent="save">
      <el-form-item label="单个分片最大条数"><el-input-number v-model="maxUnitRows" :min="50" :max="10000" :step="50" :precision="0" :disabled="busy" /><span class="field-note">最多 10,000 条，同时受 4 MiB 字节上限约束。</span></el-form-item>
      <p class="muted">上传任务时可以选择更小的分片。例如 10,000 条数据，按每片 1,000 条生成 10 片，由空闲节点依次领取。</p>
      <p class="muted">保存后仅对新任务生效；已有任务和在途分片保持原配置。</p>
      <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon />
      <el-button type="primary" native-type="submit" :loading="busy">保存分片设置</el-button>
    </el-form>
  </el-card>
  <el-card shadow="never" class="section-card"><template #header><h2>当前部署</h2></template><el-descriptions :column="1" border><el-descriptions-item label="版本">{{ session?.version }}</el-descriptions-item><el-descriptions-item label="服务角色">{{ isNode ? '执行节点' : '集群总控' }}</el-descriptions-item><el-descriptions-item label="管理登录">{{ session?.password_required ? '已启用密码登录' : '本地预览，无管理密码' }}</el-descriptions-item><el-descriptions-item label="前端">Vue 3 + Element Plus</el-descriptions-item><el-descriptions-item label="后端">Python + Flask</el-descriptions-item><el-descriptions-item label="内置任务">离线行摘要、合成模拟任务</el-descriptions-item><el-descriptions-item label="任务导出">TXT 原始记录，一行一条，保持原顺序</el-descriptions-item></el-descriptions><el-alert class="settings-note" title="数据保留策略、传输限速和设备服务尚未接入。" type="info" :closable="false" show-icon /></el-card>
</template>
