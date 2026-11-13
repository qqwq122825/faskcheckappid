<script setup lang="ts">
import { ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { session, isNode, state, busy, mutate } from '../composables/useCluster'
const maxUnitRows = ref(10000), error = ref('')
const proxyApi = ref(''), proxyScheme = ref('socks5h'), proxyError = ref(''), proxyBusy = ref(false)
const rotateAfter = ref(0), rotateBusy = ref(false)
watch(() => state.value?.settings?.device_rotate_after, value => {
  if (value != null) rotateAfter.value = value
}, { immediate: true })
watch(() => state.value?.settings?.max_unit_rows, value => {
  if (value != null) maxUnitRows.value = value
}, { immediate: true })
async function save() {
  error.value = ''
  if (!Number.isInteger(maxUnitRows.value) || maxUnitRows.value < 50 || maxUnitRows.value > 100000) {
    error.value = '单个分片最大条数应为 50–100,000 的整数'; return
  }
  try { await mutate('/api/settings', { max_unit_rows: maxUnitRows.value }, '分片上限已保存，仅对新创建的任务生效') }
  catch (e) { error.value = (e as Error).message; ElMessage.error(error.value) }
}
async function saveRotation() {
  rotateBusy.value = true
  try {
    await mutate('/api/settings/rotation', { device_rotate_after: rotateAfter.value },
                 rotateAfter.value ? `每台设备用满 ${rotateAfter.value} 条后自动换新` : '设备轮换已关闭')
  } catch (e) { ElMessage.error((e as Error).message) }
  finally { rotateBusy.value = false }
}
async function saveProxy() {
  proxyError.value = ''
  const value = proxyApi.value.trim()
  if (value && !/^https?:\/\//i.test(value)) { proxyError.value = '代理 API 需要以 http:// 或 https:// 开头'; return }
  proxyBusy.value = true
  try {
    await mutate('/api/settings/proxy', { proxy_api: value, proxy_scheme: proxyScheme.value }, '代理 API 已保存')
    proxyApi.value = ''
  } catch (e) { proxyError.value = (e as Error).message; ElMessage.error(proxyError.value) }
  finally { proxyBusy.value = false }
}
</script>
<template>
  <el-card v-if="!isNode" shadow="never" class="section-card">
    <template #header><h2>任务分片设置</h2></template>
    <el-form label-position="top" @submit.prevent="save">
      <el-form-item label="单个分片最大条数"><el-input-number v-model="maxUnitRows" :min="50" :max="100000" :step="1000" :precision="0" :disabled="busy" /><span class="field-note">最多 100,000 条，同时受 32 MiB 字节上限约束。</span></el-form-item>
      <p class="muted">上传任务时可以选择更小的分片。例如 10,000 条数据，按每片 1,000 条生成 10 片，由空闲节点依次领取。</p>
      <p class="muted">保存后仅对新任务生效；已有任务和在途分片保持原配置。</p>
      <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon />
      <el-button type="primary" native-type="submit" :loading="busy">保存分片设置</el-button>
    </el-form>
  </el-card>
  <el-card v-if="!isNode" shadow="never" class="section-card">
    <template #header><h2>设备轮换</h2></template>
    <el-form label-position="top" @submit.prevent="saveRotation">
      <el-form-item label="每台设备用满多少条后自动换新">
        <el-input-number v-model="rotateAfter" :min="0" :max="100000" :step="10" :precision="0" :disabled="rotateBusy" />
        <span class="field-note">0 表示不轮换。设备在节点本地重新注册（约 6–8 秒），旧身份被删除。</span>
      </el-form-item>
      <el-descriptions :column="1" border>
        <el-descriptions-item label="当前阈值">
          {{ state?.settings?.device_rotate_after ? state.settings.device_rotate_after + ' 条/台' : '未启用' }}
        </el-descriptions-item>
      </el-descriptions>
      <el-button type="primary" native-type="submit" :loading="rotateBusy">保存轮换设置</el-button>
    </el-form>
  </el-card>
  <el-card v-if="!isNode" shadow="never" class="section-card">
    <template #header><h2>出口代理</h2></template>
    <el-form label-position="top" @submit.prevent="saveProxy">
      <el-form-item label="代理 API 地址">
        <el-input v-model="proxyApi" type="text" spellcheck="false" :disabled="proxyBusy"
                  placeholder="粘贴提取链接，多个用空格分隔" />
        <span class="field-note">
          支持 http/https 提取接口，链接里的 num 就是单次提取上限。
          已保存时这里留空，输入新地址会覆盖。密码在页面上只显示为 ***。
        </span>
      </el-form-item>
      <el-form-item label="代理协议">
        <el-select v-model="proxyScheme" style="width:220px" :disabled="proxyBusy">
          <el-option label="socks5h（隧道住宅代理常用）" value="socks5h" />
          <el-option label="socks5" value="socks5" />
          <el-option label="http" value="http" />
          <el-option label="https" value="https" />
        </el-select>
      </el-form-item>
      <el-descriptions :column="1" border>
        <el-descriptions-item label="当前状态">
          {{ state?.settings?.proxy_configured ? '已配置' : '未配置' }}
        </el-descriptions-item>
        <el-descriptions-item label="已保存地址">
          {{ state?.settings?.proxy_api || '—' }}
        </el-descriptions-item>
      </el-descriptions>
      <el-alert v-if="proxyError" :title="proxyError" type="error" :closable="false" show-icon />
      <el-button type="primary" native-type="submit" :loading="proxyBusy">保存代理配置</el-button>
    </el-form>
  </el-card>
  <el-card shadow="never" class="section-card"><template #header><h2>当前部署</h2></template><el-descriptions :column="1" border><el-descriptions-item label="版本">{{ session?.version }}</el-descriptions-item><el-descriptions-item label="服务角色">{{ isNode ? '执行节点' : '集群总控' }}</el-descriptions-item><el-descriptions-item label="管理登录">{{ session?.password_required ? '已启用密码登录' : '本地预览，无管理密码' }}</el-descriptions-item><el-descriptions-item label="前端">Vue 3 + Element Plus</el-descriptions-item><el-descriptions-item label="后端">Python + Flask</el-descriptions-item><el-descriptions-item label="内置任务">离线行摘要、合成模拟任务</el-descriptions-item><el-descriptions-item label="任务导出">TXT 原始记录，一行一条，保持原顺序</el-descriptions-item></el-descriptions><el-alert class="settings-note" title="数据保留策略、传输限速和设备服务尚未接入。" type="info" :closable="false" show-icon /></el-card>
</template>
