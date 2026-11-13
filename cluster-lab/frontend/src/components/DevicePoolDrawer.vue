<script setup lang="ts">
import { computed, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { fmt } from '../composables/useCluster'
const visible = ref(false), node = ref<any>(null), expected = ref(0), error = ref('')
const key = computed(() => 'clusterlab.device-pool-preview.' + node.value?.id)
function show(value: any) {
  node.value = value
  expected.value = 0
  error.value = ''
  try {
    const saved = Number(localStorage.getItem(key.value))
    if (Number.isInteger(saved) && saved >= 0 && saved <= 100000) expected.value = saved
  } catch { /* Browser storage can be unavailable; the preview still opens. */ }
  visible.value = true
}
function savePreview() {
  error.value = ''
  if (!Number.isInteger(expected.value) || expected.value < 0 || expected.value > 100000) {
    error.value = '请输入 0–100,000 之间的整数'
    return
  }
  try {
    localStorage.setItem(key.value, String(expected.value))
    ElMessage.success('预计数量已保存在当前浏览器，未下发节点')
  } catch { error.value = '浏览器存储不可用，无法保存预览数量' }
}
defineExpose({ show })
</script>
<template>
  <el-drawer v-model="visible" :title="`${node?.name || ''} · 设备列表`" size="780px" class="device-pool-drawer" destroy-on-close>
    <el-alert title="设备服务未接入：这里仅预览设备池配置，不会申请或初始化真实设备。" type="info" :closable="false" show-icon />
    <section class="device-pool-target"><div class="section-heading"><h2>设备池</h2><el-tag type="info">未接入</el-tag></div><el-form label-position="top" @submit.prevent="savePreview"><el-form-item label="预计设备池数量"><el-input-number v-model="expected" :min="0" :max="100000" :step="1" :precision="0" /><span class="field-note">仅保存在当前浏览器，用于界面预览；尚未同步到总控或节点。</span></el-form-item><div class="actions"><el-button @click="savePreview">保存预览</el-button><el-button type="primary" disabled>初始化设备</el-button></div><el-alert v-if="error" :title="error" type="error" :closable="false" /></el-form></section>
    <div class="device-pool-summary"><div><span>预计数量</span><strong>{{ fmt(expected) }}</strong></div><div><span>实际设备数</span><strong>—</strong></div><div><span>就绪设备数</span><strong>—</strong></div></div>
    <section><div class="section-heading device-list-heading"><h2>设备列表</h2><span class="muted">等待设备服务接入</span></div><el-table :data="[]" empty-text="尚未接入设备服务，暂无可显示的设备" stripe><el-table-column prop="id" label="设备编号" min-width="160" /><el-table-column prop="status" label="状态" min-width="110" /><el-table-column prop="initialized" label="初始化时间" min-width="180" /></el-table></section>
    <template #footer><el-button @click="visible = false">关闭</el-button></template>
  </el-drawer>
</template>
