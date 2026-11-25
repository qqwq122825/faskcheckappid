<script setup lang="ts">
import { ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { session, isNode, state, busy, mutate } from '../composables/useCluster'
const maxUnitRows = ref(10000), error = ref('')
const proxyApi = ref(''), proxyScheme = ref('socks5h'), proxyError = ref(''), proxyBusy = ref(false)
const proxyDebug = ref(false), proxyDebugBusy = ref(false)
const webSecondaryAuth = ref(true), webSecondaryAuthBusy = ref(false)
const publicHeaderUseLimit = ref(15), deviceUseLimit = ref(1000)
const policyBusy = ref(false), policyError = ref('')
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
watch(() => state.value?.settings?.public_header_use_limit, value => {
  if (value != null) publicHeaderUseLimit.value = Number(value)
}, { immediate: true })
watch(() => state.value?.settings?.device_use_limit, value => {
  if (value != null) deviceUseLimit.value = Number(value)
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
async function saveDevicePolicy() {
  policyError.value = ''
  if (!Number.isInteger(publicHeaderUseLimit.value) || publicHeaderUseLimit.value < 1) {
    policyError.value = '公共头使用次数必须是正整数'; return
  }
  if (!Number.isInteger(deviceUseLimit.value) || deviceUseLimit.value < 1) {
    policyError.value = '设备使用次数必须是正整数'; return
  }
  policyBusy.value = true
  try {
    await mutate('/api/settings/device-policy', {
      public_header_use_limit: publicHeaderUseLimit.value,
      device_use_limit: deviceUseLimit.value
    }, '设备策略已保存，对新领取的工作包生效')
  } catch (e) { policyError.value = (e as Error).message; ElMessage.error(policyError.value) }
  finally { policyBusy.value = false }
}
</script>
<template><el-card shadow="never" class="settings-card">
  <template #header><div class="settings-title"><div><h2>系统设置</h2><p>常用配置集中在一个页面，高级选项默认收起。</p></div><el-tag effect="plain">v{{ session?.version }}</el-tag></div></template>

  <template v-if="!isNode">
    <section class="setting-cell">
      <div class="cell-copy"><h3>任务分片</h3><p>只影响新任务；已有任务保持原配置。</p></div>
      <form class="cell-control inline-control" @submit.prevent="save">
        <el-input-number v-model="maxUnitRows" :min="50" :max="100000" :step="1000" :precision="0" :disabled="busy" />
        <el-button type="primary" native-type="submit" :loading="busy">保存</el-button>
        <span class="control-note">50–100,000 条，且不超过 32 MiB</span>
      </form>
      <el-alert v-if="error" class="cell-alert" :title="error" type="error" :closable="false" show-icon />
    </section>

    <section class="setting-cell setting-cell--top">
      <div class="cell-copy"><h3>出口代理</h3><p>每个节点自动轮询使用一条代理 API。</p></div>
      <form class="cell-control proxy-control" @submit.prevent="addProxy">
        <div class="proxy-input-row">
          <el-input v-model="proxyApi" type="textarea" :autosize="{ minRows: 2, maxRows: 3 }" spellcheck="false" :disabled="proxyBusy" placeholder="粘贴代理提取链接；多个链接时每行一个" />
          <div class="proxy-actions"><el-select v-model="proxyScheme" :disabled="proxyBusy"><el-option label="socks5h" value="socks5h" /><el-option label="socks5" value="socks5" /><el-option label="http" value="http" /><el-option label="https" value="https" /></el-select><el-button type="primary" native-type="submit" :loading="proxyBusy">添加代理配置</el-button></div>
        </div>
        <p class="control-note">任务开始先按线程数取满代理，水位低于 50% 时补到目标；新代理优先，其余按队列平均轮转复用。按节点创建顺序轮询分配，密码仅显示为 ***。</p>
        <div v-if="state?.settings?.proxy_apis?.length" class="proxy-list">
          <div v-for="(api, index) in state.settings.proxy_apis" :key="`${index}-${api}`" class="proxy-row"><span class="proxy-index">{{ Number(index) + 1 }}</span><code>{{ api }}</code><span class="proxy-node">节点 {{ Number(index) + 1 }}、{{ Number(index) + 1 + Number(state.settings.proxy_count) }}…</span><el-button type="danger" text :disabled="proxyBusy" @click="removeProxy(Number(index))">删除</el-button></div>
        </div>
        <el-alert v-if="proxyError" :title="proxyError" type="error" :closable="false" show-icon />
      </form>
    </section>

    <section class="setting-cell">
      <div class="cell-copy"><h3>设备策略</h3><p>两个阈值统一下发到节点。</p></div>
      <form class="cell-control policy-control" @submit.prevent="saveDevicePolicy">
        <label><span>公共头刷新</span><el-input-number v-model="publicHeaderUseLimit" :min="1" :max="10000" :precision="0" :disabled="policyBusy" /><small>个账号后</small></label>
        <label><span>设备淘汰</span><el-input-number v-model="deviceUseLimit" :min="1" :max="1000000" :step="100" :precision="0" :disabled="policyBusy" /><small>个账号后</small></label>
        <el-button type="primary" native-type="submit" :loading="policyBusy">保存</el-button>
        <p class="control-note">公共头到阈值后由线程自动更新；设备到阈值后删除旧身份，下一个账号使用当前出口重新注册。</p>
        <el-alert v-if="policyError" :title="policyError" type="error" :closable="false" show-icon />
      </form>
    </section>

    <el-collapse class="advanced-settings">
      <el-collapse-item name="advanced">
        <template #title><div class="advanced-title"><span>高级设置</span><small>二次验证、诊断日志和部署信息</small></div></template>
        <section class="setting-cell">
          <div class="cell-copy"><h3>Web 二次验证</h3><p>仅对 GSA 返回 au=repair 的新工作包做细分。</p></div>
          <div class="cell-control switch-control"><el-switch v-model="webSecondaryAuth" :loading="webSecondaryAuthBusy" :disabled="webSecondaryAuthBusy" active-text="开启" inactive-text="关闭" @change="saveWebSecondaryAuth" /><span class="control-note">authType=sa/non-sa 归入密码正确；hsa/hsa2 保留为双重验证。Web HTTP 0 或 503 按任务设置的重试次数重试。</span></div>
        </section>
        <section class="setting-cell">
          <div class="cell-copy"><h3>代理调试日志</h3><p>仅排查代理问题时开启。</p></div>
          <div class="cell-control switch-control"><el-switch v-model="proxyDebug" :loading="proxyDebugBusy" :disabled="proxyDebugBusy" active-text="开启" inactive-text="关闭" @change="saveProxyDebug" /><span class="control-note">开启后节点直接回传 Apple 原始英文原因并写入 proxy-debug.jsonl；关闭后节点只回传快捷分类，总控显示中文原因。避免产生大量日志。</span></div>
        </section>
        <section class="setting-cell deployment-cell"><div class="cell-copy"><h3>当前部署</h3><p>服务与登录状态。</p></div><div class="cell-control deployment-values"><span>v{{ session?.version }}</span><span>{{ isNode ? '执行节点' : '集群总控' }}</span><span>{{ session?.password_required ? '密码登录已启用' : '未启用密码登录' }}</span></div></section>
      </el-collapse-item>
    </el-collapse>
  </template>
  <section v-else class="setting-cell deployment-cell"><div class="cell-copy"><h3>当前部署</h3><p>节点本地只显示部署信息。</p></div><div class="cell-control deployment-values"><span>v{{ session?.version }}</span><span>执行节点</span><span>{{ session?.password_required ? '密码登录已启用' : '未启用密码登录' }}</span></div></section>
</el-card></template>
<style scoped>
.settings-card{max-width:1180px;margin:0 auto;border-radius:14px}.settings-card :deep(.el-card__header){padding:22px 26px}.settings-card :deep(.el-card__body){padding:0}.settings-title{display:flex;align-items:center;justify-content:space-between;gap:20px}.settings-title h2,.cell-copy h3{margin:0}.settings-title p,.cell-copy p{margin:6px 0 0;color:var(--el-text-color-secondary)}.setting-cell{display:grid;grid-template-columns:210px minmax(0,1fr);gap:26px;padding:24px 26px;border-top:1px solid var(--el-border-color-lighter);align-items:center}.setting-cell--top{align-items:start}.cell-copy h3{font-size:17px}.cell-copy p{font-size:13px;line-height:1.55}.cell-control{min-width:0}.inline-control{display:flex;align-items:center;gap:12px;flex-wrap:wrap}.control-note{color:var(--el-text-color-secondary);font-size:13px;line-height:1.55}.cell-alert{grid-column:2}.proxy-input-row{display:grid;grid-template-columns:minmax(0,1fr) 130px;gap:12px}.proxy-actions{display:flex;flex-direction:column;gap:10px}.proxy-control>.control-note{display:block;margin:10px 0}.proxy-list{border:1px solid var(--el-border-color-lighter);border-radius:9px;overflow:hidden;margin-top:12px}.proxy-row{display:grid;grid-template-columns:34px minmax(0,1fr) 150px 60px;gap:10px;align-items:center;padding:10px 12px}.proxy-row+.proxy-row{border-top:1px solid var(--el-border-color-lighter)}.proxy-index{color:var(--el-text-color-secondary)}.proxy-row code{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12px}.proxy-node{color:var(--el-text-color-secondary);font-size:12px}.policy-strip,.deployment-values{display:flex;gap:10px;flex-wrap:wrap}.policy-strip span,.deployment-values span{padding:10px 14px;border:1px solid var(--el-border-color-lighter);border-radius:9px;background:var(--el-fill-color-lighter);font-size:13px}.advanced-settings{border-top:1px solid var(--el-border-color-lighter);border-bottom:0}.advanced-settings :deep(.el-collapse-item__header){height:58px;padding:0 26px;border-bottom:0}.advanced-settings :deep(.el-collapse-item__content){padding:0}.advanced-settings :deep(.el-collapse-item__wrap){border-bottom:0}.advanced-title{display:flex;align-items:center;gap:12px;font-weight:600}.advanced-title small{font-weight:400;color:var(--el-text-color-secondary)}.switch-control{display:flex;align-items:center;gap:16px}.deployment-cell{padding-bottom:26px}@media(max-width:760px){.setting-cell{grid-template-columns:1fr;gap:14px;padding:20px}.cell-alert{grid-column:1}.proxy-input-row{grid-template-columns:1fr}.proxy-actions{flex-direction:row}.proxy-actions :deep(.el-select){flex:1}.proxy-row{grid-template-columns:24px minmax(0,1fr) 48px}.proxy-node{display:none}.switch-control{align-items:flex-start;flex-direction:column}}
</style>
