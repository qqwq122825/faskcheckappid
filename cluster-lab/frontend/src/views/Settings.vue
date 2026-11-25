<script setup lang="ts">
import { ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { session, isNode, state, busy, mutate } from '../composables/useCluster'
const maxUnitRows = ref(10000), error = ref('')
const proxyApi = ref(''), proxyScheme = ref('socks5h'), proxyError = ref(''), proxyBusy = ref(false)
const proxyDebug = ref(false), proxyDebugBusy = ref(false)
const webSecondaryAuth = ref(true), webSecondaryAuthBusy = ref(false)
watch(() => state.value?.settings?.max_unit_rows, value => {
  if (value != null) maxUnitRows.value = value
}, { immediate: true })
watch(() => state.value?.settings?.proxy_scheme, value => {
  if (value) proxyScheme.value = value
}, { immediate: true })
watch(() => state.value?.settings?.proxy_debug, value => {
  if (value != null) proxyDebug.value = Boolean(value)
}, { immediate: true })
watch(() => state.value?.settings?.web_secondary_auth, value => {
  if (value != null) webSecondaryAuth.value = Boolean(value)
}, { immediate: true })
async function save() {
  error.value = ''
  if (!Number.isInteger(maxUnitRows.value) || maxUnitRows.value < 50 || maxUnitRows.value > 100000) {
    error.value = '单个分片最大条数应为 50–100,000 的整数'; return
  }
  try { await mutate('/api/settings', { max_unit_rows: maxUnitRows.value }, '分片上限已保存，仅对新创建的任务生效') }
  catch (e) { error.value = (e as Error).message; ElMessage.error(error.value) }
}
async function addProxy() {
  proxyError.value = ''
  const value = proxyApi.value.trim()
  if (!value) { proxyError.value = '请填写要添加的代理 API 地址'; return }
  if (!/^https?:\/\//i.test(value)) { proxyError.value = '代理 API 需要以 http:// 或 https:// 开头'; return }
  proxyBusy.value = true
  try {
    await mutate('/api/settings/proxy', { action: 'add', proxy_api: value, proxy_scheme: proxyScheme.value }, '代理配置已添加')
    proxyApi.value = ''
  } catch (e) { proxyError.value = (e as Error).message; ElMessage.error(proxyError.value) }
  finally { proxyBusy.value = false }
}
async function removeProxy(index: number) {
  proxyBusy.value = true
  try { await mutate('/api/settings/proxy', { action: 'delete', index }, '代理配置已删除') }
  catch (e) { proxyError.value = (e as Error).message; ElMessage.error(proxyError.value) }
  finally { proxyBusy.value = false }
}
async function saveProxyDebug(value: string | number | boolean) {
  const enabled = Boolean(value)
  proxyDebugBusy.value = true
  try {
    await mutate('/api/settings/debug', { proxy_debug: enabled },
                 enabled ? '代理调试日志已开启，将对新工作包生效' : '代理调试日志已关闭，新工作包不再记录')
  } catch (e) {
    proxyDebug.value = !enabled
    ElMessage.error((e as Error).message)
  } finally { proxyDebugBusy.value = false }
}
async function saveWebSecondaryAuth(value: string | number | boolean) {
  const enabled = Boolean(value)
  webSecondaryAuthBusy.value = true
  try {
    await mutate('/api/settings/web-secondary-auth', { web_secondary_auth: enabled },
                 enabled ? 'Web 二次验证已开启，仅对 au=repair 的新工作包生效' : 'Web 二次验证已关闭')
  } catch (e) {
    webSecondaryAuth.value = !enabled
    ElMessage.error((e as Error).message)
  } finally { webSecondaryAuthBusy.value = false }
}
</script>
<template><div class="settings-grid">
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
    <template #header><h2>Web 二次验证</h2></template>
    <el-form label-position="top">
      <el-form-item label="细分 GSA au=repair 结果">
        <el-switch v-model="webSecondaryAuth" :loading="webSecondaryAuthBusy" :disabled="webSecondaryAuthBusy"
                   active-text="开启 Web 校验" inactive-text="关闭 Web 校验"
                   @change="saveWebSecondaryAuth" />
        <span class="field-note">
          仅对 GSA 返回 au=repair 的账号追加一次 Web 校验。authType=sa/non-sa 归入密码正确；
          hsa/hsa2 保留为双重验证。Web HTTP 0 或 503 按任务设置的重试次数重试。
        </span>
      </el-form-item>
      <el-descriptions :column="1" border>
        <el-descriptions-item label="当前状态">{{ webSecondaryAuth ? '已开启' : '已关闭' }}</el-descriptions-item>
        <el-descriptions-item label="生效范围">新领取的工作包</el-descriptions-item>
      </el-descriptions>
    </el-form>
  </el-card>
  <el-card v-if="!isNode" shadow="never" class="section-card">
    <template #header><h2>出口代理</h2></template>
    <el-form label-position="top" @submit.prevent="addProxy">
      <el-form-item label="代理 API 地址">
        <el-input v-model="proxyApi" type="textarea" :autosize="{ minRows: 2, maxRows: 5 }"
                  spellcheck="false" :disabled="proxyBusy"
                  placeholder="粘贴提取链接；也可一次粘贴多个，每行一个" />
        <span class="field-note">
          最多 16 个。总控按节点创建顺序轮询分配。每个工作线程启动时直接提取 1 条代理，不再维护 IP 池。
          密码在页面上只显示为 ***。
        </span>
      </el-form-item>
      <el-form-item label="代理协议（全部配置共用）">
        <el-select v-model="proxyScheme" style="width:220px" :disabled="proxyBusy">
          <el-option label="socks5h（隧道住宅代理常用）" value="socks5h" />
          <el-option label="socks5" value="socks5" />
          <el-option label="http" value="http" />
          <el-option label="https" value="https" />
        </el-select>
      </el-form-item>
      <el-descriptions :column="1" border>
        <el-descriptions-item label="当前状态">
          {{ state?.settings?.proxy_configured ? `已配置 ${state.settings.proxy_count} 个` : '未配置' }}
        </el-descriptions-item>
      </el-descriptions>
      <el-table v-if="state?.settings?.proxy_apis?.length" :data="state.settings.proxy_apis" border>
        <el-table-column type="index" label="#" width="60" />
        <el-table-column label="已保存代理 API" min-width="520">
          <template #default="scope"><span class="proxy-api-value">{{ scope.row }}</span></template>
        </el-table-column>
        <el-table-column label="分配" width="120"><template #default="scope">节点 {{ scope.$index + 1 }}、{{ scope.$index + 1 + state.settings.proxy_count }}…</template></el-table-column>
        <el-table-column label="操作" width="100"><template #default="scope"><el-button type="danger" text :disabled="proxyBusy" @click="removeProxy(scope.$index)">删除</el-button></template></el-table-column>
      </el-table>
      <el-alert v-if="proxyError" :title="proxyError" type="error" :closable="false" show-icon />
      <el-button type="primary" native-type="submit" :loading="proxyBusy">添加代理配置</el-button>
    </el-form>
  </el-card>
  <el-card v-if="!isNode" shadow="never" class="section-card">
    <template #header><h2>代理调试日志</h2></template>
    <el-form label-position="top">
      <el-form-item label="记录超时代理与实际出口 IP">
        <el-switch v-model="proxyDebug" :loading="proxyDebugBusy" :disabled="proxyDebugBusy"
                   active-text="开启调试日志" inactive-text="关闭调试日志"
                   @change="saveProxyDebug" />
        <span class="field-note">
          开启后，节点直接回传 Apple 原始英文原因，结果页不再用中文覆盖，并额外记录代理出口与诊断详情；关闭后，节点只回传快捷分类，总控显示中文原因，新工作包不再创建 proxy-debug.jsonl，已有日志保留。
          仅排查代理问题时开启，避免产生大量日志。
        </span>
      </el-form-item>
      <el-descriptions :column="1" border>
        <el-descriptions-item label="当前状态">{{ proxyDebug ? '已开启' : '已关闭' }}</el-descriptions-item>
        <el-descriptions-item label="生效范围">新领取的工作包</el-descriptions-item>
      </el-descriptions>
    </el-form>
  </el-card>
  <el-card shadow="never" class="section-card"><template #header><h2>当前部署</h2></template><el-descriptions :column="1" border><el-descriptions-item label="版本">{{ session?.version }}</el-descriptions-item><el-descriptions-item label="服务角色">{{ isNode ? '执行节点' : '集群总控' }}</el-descriptions-item><el-descriptions-item label="管理登录">{{ session?.password_required ? '已启用密码登录' : '未启用密码登录' }}</el-descriptions-item><el-descriptions-item label="任务导出">TXT 原始记录，一行一条，保持原顺序</el-descriptions-item></el-descriptions></el-card>
</div></template>
<style scoped>
.settings-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); gap: 20px; align-items: start; }
.settings-grid :deep(.section-card) { margin: 0; min-width: 0; }
@media (max-width: 760px) { .settings-grid { grid-template-columns: 1fr; } }
</style>
