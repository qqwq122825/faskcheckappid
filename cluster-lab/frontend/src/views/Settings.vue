<script setup lang="ts">
import { ref, watch } from 'vue'
import { Grid, Connection, Setting, InfoFilled } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import { session, isNode, state, busy, mutate } from '../composables/useCluster'
const maxUnitRows = ref(10000), error = ref('')
const proxyApi = ref(''), proxyScheme = ref('socks5h'), proxyError = ref(''), proxyBusy = ref(false)
const webSecondaryAuth = ref(true), webSecondaryAuthBusy = ref(false)
watch(() => state.value?.settings?.max_unit_rows, value => {
  if (value != null) maxUnitRows.value = value
}, { immediate: true })
watch(() => state.value?.settings?.proxy_scheme, value => {
  if (value) proxyScheme.value = value
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
<template>
  <div class="settings-page">
    <template v-if="!isNode">
      <section class="settings-card shard-card">
        <div class="card-heading">
          <span class="setting-icon"><el-icon><Grid /></el-icon></span>
          <div><h2>任务分片</h2><p>设置新任务的分片上限，已有任务不受影响。</p></div>
          <span class="scope-tag">全局配置</span>
        </div>
        <form class="shard-form" @submit.prevent="save">
          <label for="max-unit-rows">每个分片最多记录数 <span>50–100,000 条 · 最大 32 MiB</span></label>
          <div class="shard-control"><el-input-number id="max-unit-rows" v-model="maxUnitRows" :min="50" :max="100000" :step="1000" :precision="0" :disabled="busy" /><span class="input-unit">条 / 分片</span><el-button type="primary" native-type="submit" :loading="busy">保存配置</el-button></div>
          <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon />
        </form>
        <div class="task-config-note"><el-icon><InfoFilled /></el-icon><span>公共头刷新、设备淘汰已移至<strong>创建任务 · 设备策略</strong>，可按任务单独配置。</span></div>
      </section>

      <section class="settings-card proxy-card">
        <div class="card-heading">
          <span class="setting-icon"><el-icon><Connection /></el-icon></span>
          <div><h2>出口代理</h2><p>配置代理提取 API，按节点创建顺序轮询分配。</p></div>
          <span class="count-tag">{{ state?.settings?.proxy_apis?.length || 0 }} 个配置</span>
        </div>
        <form class="proxy-form" @submit.prevent="addProxy">
          <div class="proxy-input-row">
            <div class="proxy-address"><label for="proxy-api">代理 API 地址</label><el-input id="proxy-api" v-model="proxyApi" type="textarea" :autosize="{ minRows: 2, maxRows: 4 }" spellcheck="false" :disabled="proxyBusy" placeholder="粘贴代理提取链接，多个链接时每行一个" /></div>
            <div class="proxy-actions"><label for="proxy-scheme">代理协议</label><el-select id="proxy-scheme" v-model="proxyScheme" aria-label="代理协议" :disabled="proxyBusy"><el-option label="socks5h" value="socks5h" /><el-option label="socks5" value="socks5" /><el-option label="http" value="http" /><el-option label="https" value="https" /></el-select><el-button type="primary" native-type="submit" :loading="proxyBusy">添加代理配置</el-button></div>
          </div>
          <el-alert v-if="proxyError" :title="proxyError" type="error" :closable="false" show-icon />
        </form>
        <div class="proxy-list">
          <div class="proxy-list-heading"><span>已添加的代理</span><span>按节点创建顺序分配 · 密码已隐藏</span></div>
          <div v-for="(api, index) in state?.settings?.proxy_apis || []" :key="`${index}-${api}`" class="proxy-row"><span class="proxy-index">{{ Number(index) + 1 }}</span><div class="proxy-row-main"><code :title="api">{{ api }}</code><span>分配至节点 {{ Number(index) + 1 }}、{{ Number(index) + 1 + Number(state?.settings?.proxy_count || 0) }}…</span></div><el-button type="danger" text :disabled="proxyBusy" :aria-label="`删除代理 ${Number(index) + 1}`" @click="removeProxy(Number(index))">删除</el-button></div>
          <div v-if="!state?.settings?.proxy_apis?.length" class="proxy-empty">暂无代理配置，在上方添加提取链接即可。</div>
        </div>
        <details class="proxy-help"><summary>代理如何分配与补充？</summary><p>任务开始先按线程数取满代理，水位低于 50% 时补到目标。新代理优先，其余按队列平均轮转复用；每个节点自动轮询使用一条代理 API。</p></details>
      </section>

      <el-collapse class="advanced-settings">
        <el-collapse-item name="advanced">
          <template #title><div class="advanced-title"><span class="setting-icon neutral"><el-icon><Setting /></el-icon></span><div><strong>高级设置</strong><span>二次验证、诊断日志与部署信息</span></div></div></template>
          <section class="advanced-row">
            <div><h3>Web 二次验证</h3><p>仅对 GSA 返回 au=repair 的新工作包做细分。</p><small>authType=sa/non-sa 归入密码正确；hsa/hsa2 保留为双重验证。Web HTTP 0 或 503 按任务设置的重试次数重试。</small></div>
            <el-switch v-model="webSecondaryAuth" aria-label="Web 二次验证" :loading="webSecondaryAuthBusy" :disabled="webSecondaryAuthBusy" @change="saveWebSecondaryAuth" />
          </section>
          <div class="deployment-values"><span>当前版本 <strong>v{{ session?.version }}</strong></span><span>部署角色 <strong>集群总控</strong></span><span>{{ session?.password_required ? '密码登录已启用' : '未启用密码登录' }}</span></div>
        </el-collapse-item>
      </el-collapse>
    </template>
    <section v-else class="settings-card"><div class="card-heading"><span class="setting-icon"><el-icon><InfoFilled /></el-icon></span><div><h2>当前部署</h2><p>节点本地只显示部署信息。</p></div></div><div class="deployment-values"><span>v{{ session?.version }}</span><span>执行节点</span><span>{{ session?.password_required ? '密码登录已启用' : '未启用密码登录' }}</span></div></section>
  </div>
</template>
<style scoped>
.settings-page{max-width:1120px;margin:0 auto;display:grid;gap:20px;padding-bottom:32px;color:#293b45}
.settings-card{background:#fff;border:1px solid #e2e8ec;border-radius:14px;overflow:hidden;box-shadow:0 2px 8px #253c4912}
.card-heading{display:flex;align-items:center;gap:14px;padding:24px 28px;border-bottom:1px solid #edf1f3}.card-heading h2{font-size:18px;line-height:1.4;font-weight:650;margin:0}.card-heading p{font-size:14px;line-height:1.6;color:#73838e;margin:5px 0 0}.setting-icon{width:42px;height:42px;display:grid;place-items:center;flex-shrink:0;background:#edf8f2;color:#209b68;border-radius:12px;font-size:21px}.setting-icon.neutral{background:#f0f3f6;color:#71818e}.scope-tag,.count-tag{margin-left:auto;white-space:nowrap;color:#667983;background:#f3f6f8;padding:5px 11px;border-radius:6px;font-size:13px}.count-tag{color:#22845c;background:#eff8f2}
.shard-form{padding:22px 28px;display:grid;gap:13px}.shard-form>label{font-size:14px;font-weight:600}.shard-form>label>span{font-weight:400;color:#7b8a94;font-size:13px;margin-left:16px}.shard-control{display:flex;gap:12px;align-items:center}.shard-control .el-input-number{width:210px}.shard-control .el-button{margin-left:auto}.input-unit{color:#7b8a94;font-size:14px}.task-config-note{display:flex;gap:9px;align-items:center;background:#f8fafb;border-top:1px solid #edf1f3;padding:13px 28px;font-size:13px;color:#7b8a94;line-height:1.65}.task-config-note .el-icon{color:#7b96a5;flex-shrink:0}.task-config-note strong{color:#526d61;font-weight:500;margin:0 4px}
.proxy-form{padding:22px 28px}.proxy-form>.el-alert{margin-top:14px}.proxy-input-row{display:grid;grid-template-columns:minmax(0,1fr) 168px;gap:16px}.proxy-address,.proxy-actions{display:flex;flex-direction:column;gap:9px}.proxy-address>label,.proxy-actions>label{font-size:14px;font-weight:600}.proxy-actions .el-select{width:100%}.proxy-actions .el-button{margin:0}.proxy-address :deep(.el-textarea__inner){min-height:94px!important;line-height:1.7;padding:10px 13px;font-size:14px}.proxy-list{margin:0 28px;border:1px solid #e7edf0;border-radius:9px;overflow:hidden}.proxy-list-heading{display:flex;justify-content:space-between;gap:12px;padding:11px 16px;background:#f7f9fb;font-size:13px;color:#6c7d87}.proxy-list-heading span:first-child{font-weight:600;color:#4d626e}.proxy-row{display:flex;align-items:center;gap:14px;padding:16px;border-top:1px solid #e9eef1}.proxy-index{display:grid;place-items:center;width:28px;height:28px;flex-shrink:0;border-radius:7px;background:#f2f6f8;color:#81939e;font-size:13px}.proxy-row-main{min-width:0;flex:1;display:grid;gap:6px}.proxy-row-main code{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:13px;color:#405662}.proxy-row-main>span{font-size:12px;color:#87959f}.proxy-empty{padding:24px 16px;font-size:14px;color:#81909a;text-align:center}.proxy-help{padding:17px 28px 20px;color:#72848f;font-size:13px;line-height:1.7}.proxy-help summary{cursor:pointer;max-width:fit-content}.proxy-help p{margin:9px 0 0;color:#80909a}
.advanced-settings{padding:0 28px;border:1px solid #e2e8ec;border-radius:14px;overflow:hidden}.advanced-settings :deep(.el-collapse-item__header){height:86px;border:0;background:transparent}.advanced-settings :deep(.el-collapse-item__wrap){border:0}.advanced-settings :deep(.el-collapse-item__content){padding-bottom:0}.advanced-title{display:flex;align-items:center;gap:14px;line-height:1.5;text-align:left}.advanced-title>div{display:grid;gap:4px}.advanced-title strong{font-size:16px;font-weight:600}.advanced-title>div>span{font-size:13px;font-weight:400;color:#7b8a95}.advanced-row{display:flex;align-items:center;justify-content:space-between;gap:28px;padding:21px 0;border-top:1px solid #edf1f3}.advanced-row h3{font-size:15px;margin:0}.advanced-row p{font-size:14px;line-height:1.6;color:#73838e;margin:6px 0}.advanced-row small{font-size:13px;color:#8a98a1;line-height:1.6}.advanced-row .el-switch{flex-shrink:0}.deployment-values{display:flex;gap:18px 30px;flex-wrap:wrap;padding:18px 28px;background:#f8fafb;border-top:1px solid #edf1f3;color:#7b8a95;font-size:13px}.advanced-settings .deployment-values{margin:0 -28px}.deployment-values strong{color:#49616d;font-weight:500;margin-left:7px}
@media(max-width:700px){.settings-page{gap:14px}.card-heading{padding:18px;gap:11px}.card-heading p{font-size:13px}.scope-tag{display:none}.setting-icon{width:36px;height:36px;border-radius:10px;font-size:19px}.shard-form,.proxy-form{padding:18px}.shard-form>label>span{display:block;margin:6px 0 0}.shard-control{flex-wrap:wrap;gap:10px}.shard-control .el-input-number{width:calc(100% - 80px)}.shard-control .el-button{width:100%;margin:4px 0 0}.task-config-note{padding:12px 18px;align-items:flex-start}.task-config-note .el-icon{margin-top:4px}.proxy-input-row{grid-template-columns:1fr}.proxy-actions{display:grid;grid-template-columns:1fr 1fr}.proxy-actions>label{grid-column:1/-1}.proxy-list{margin:0 18px}.proxy-list-heading{padding:10px 12px}.proxy-list-heading span:last-child{display:none}.proxy-row{padding:12px;gap:8px}.proxy-row-main code{font-size:12px}.proxy-help{padding:16px 18px}.advanced-settings{padding:0 18px}.advanced-title{gap:10px}.advanced-title>div>span{font-size:12px}.advanced-row{gap:16px}.deployment-values{padding:16px 18px;gap:12px 20px}.advanced-settings .deployment-values{margin:0 -18px}}
</style>
