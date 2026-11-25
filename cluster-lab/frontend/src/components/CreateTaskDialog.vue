<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { UploadFilled, DocumentAdd, Timer, Refresh, InfoFilled } from '@element-plus/icons-vue'
import { api, busy, createTaskOpen, refresh, session, state } from '../composables/useCluster'
const router = useRouter()
const name = ref(''), unitRows = ref(10000), file = ref<File>(), error = ref('')
const files = ref<any[]>([]), chosen = ref(''), mode = ref<'new' | 'reuse'>('new')
const timeout = ref(25), retries = ref(2)
const publicHeaderUseLimit = ref(15), deviceUseLimit = ref(1000)
const uploadPercent = ref<number | null>(null)
const maxUnitRows = computed(() => state.value?.settings?.max_unit_rows || 10000)
watch(createTaskOpen, open => {
  if (open) {
    error.value = ''; file.value = undefined; name.value = ''
    unitRows.value = Math.min(10000, maxUnitRows.value)
    timeout.value = 25; retries.value = 2
    publicHeaderUseLimit.value = Number(state.value?.settings?.public_header_use_limit || 15)
    deviceUseLimit.value = Number(state.value?.settings?.device_use_limit || 1000)
    uploadPercent.value = null
    chosen.value = ''; mode.value = 'new'
    loadFiles()
  }
})
function chooseFile(upload: { raw?: File }) {
  file.value = upload.raw
  error.value = ''
  if (file.value && !file.value.name.toLowerCase().endsWith('.txt')) {
    file.value = undefined
    error.value = '请选择 .txt 文本文件'
  }
}
async function loadFiles() {
  try { files.value = (await api('/api/files')).files || [] } catch { files.value = [] }
}
function sizeText(bytes: number) {
  if (!bytes) return '—'
  const mb = bytes / 1024 / 1024
  return mb >= 1024 ? (mb / 1024).toFixed(2) + ' GB' : mb >= 1 ? mb.toFixed(1) + ' MB' : (bytes / 1024).toFixed(0) + ' KB'
}
function uploadStream(selected: File): Promise<any> {
  const query = new URLSearchParams({ name: name.value.trim() || selected.name,
    unit_rows: String(unitRows.value), mode: 'auth', timeout: String(timeout.value),
    retries: String(retries.value), item_timeout: String(timeout.value),
    public_header_use_limit: String(publicHeaderUseLimit.value), device_use_limit: String(deviceUseLimit.value),
    enqueue: 'true' })
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest()
    xhr.open('POST', `/api/jobs/stream?${query}`)
    xhr.setRequestHeader('X-CSRF-Token', session.value?.csrf || '')
    xhr.setRequestHeader('Content-Type', 'application/octet-stream')
    xhr.upload.onprogress = event => {
      if (event.lengthComputable) uploadPercent.value = Math.min(100, Math.round(event.loaded / event.total * 100))
    }
    xhr.onerror = () => reject(new Error('上传连接中断'))
    xhr.onload = () => {
      let result: any = {}
      try { result = JSON.parse(xhr.responseText || '{}') } catch { /* use status below */ }
      if (xhr.status >= 200 && xhr.status < 300) resolve(result)
      else reject(new Error(result.error || `上传失败（${xhr.status}）`))
    }
    xhr.send(selected)
  })
}
async function submit() {
  if (busy.value) return
  error.value = ''
  if (!Number.isInteger(publicHeaderUseLimit.value) || publicHeaderUseLimit.value < 1 || publicHeaderUseLimit.value > 10000) {
    error.value = '公共头刷新应为 1–10,000 个账号'; return
  }
  if (!Number.isInteger(deviceUseLimit.value) || deviceUseLimit.value < 1 || deviceUseLimit.value > 1000000) {
    error.value = '设备淘汰应为 1–1,000,000 个账号'; return
  }
  if (!Number.isInteger(timeout.value) || timeout.value < 5 || timeout.value > 120 ||
      !Number.isInteger(retries.value) || retries.value < 0 || retries.value > 5) {
    error.value = '请检查请求策略：账号超时 5–120 秒，重试 0–5 次'; return
  }
  if (mode.value === 'reuse') {
    if (!chosen.value) { error.value = '请选择一个已上传的文件'; return }
    if (!Number.isInteger(unitRows.value) || unitRows.value < 50 || unitRows.value > maxUnitRows.value) {
      error.value = `每个分片应为 50–${maxUnitRows.value.toLocaleString('zh-CN')} 条`; return
    }
    busy.value = true
    try {
      await api('/api/jobs/from-file', { source: chosen.value, name: name.value.trim(),
                                         unit_rows: unitRows.value, mode: 'auth', enqueue: 'true',
                                         timeout: timeout.value, retries: retries.value,
                                         item_timeout: timeout.value,
                                         public_header_use_limit: publicHeaderUseLimit.value,
                                         device_use_limit: deviceUseLimit.value }, 'POST')
      createTaskOpen.value = false
      await refresh()
      await router.push('/jobs')
    } catch (e) { error.value = (e as Error).message }
    finally { busy.value = false }
    return
  }
  if (!file.value) { error.value = '请先选择 TXT 文件'; return }
  if (!Number.isInteger(unitRows.value) || unitRows.value < 50 || unitRows.value > maxUnitRows.value) {
    error.value = `每个分片应为 50–${maxUnitRows.value.toLocaleString('zh-CN')} 条，请检查总控分片上限`; return
  }
  busy.value = true
  try {
    uploadPercent.value = 0
    await uploadStream(file.value)
    createTaskOpen.value = false
    await refresh()
    await router.push('/jobs')
  } catch (e) { error.value = (e as Error).message }
  finally { busy.value = false; uploadPercent.value = null }
}
</script>
<template>
  <el-dialog v-model="createTaskOpen" class="create-task-dialog" title="创建任务" width="960px" top="6vh" :close-on-click-modal="!busy" :close-on-press-escape="!busy" :show-close="!busy" destroy-on-close>
    <template #header><div class="create-dialog-heading"><span class="create-dialog-icon"><el-icon><DocumentAdd /></el-icon></span><div><h2>创建任务</h2><p>选择数据文件，配置本次任务的执行策略。</p></div></div></template>
    <el-form id="create-task-form" class="create-task-form" label-position="top" novalidate @submit.prevent="submit">
      <div class="create-task-columns">
        <section class="source-panel">
          <div class="task-section-heading"><h3>任务数据</h3><span>TXT 文本文件</span></div>
          <el-radio-group v-model="mode" class="source-toggle" aria-label="输入来源" :disabled="busy">
            <el-radio-button value="new">上传新文件</el-radio-button><el-radio-button value="reuse" :disabled="!files.length">使用已上传文件</el-radio-button>
          </el-radio-group>
          <el-form-item v-if="mode === 'reuse'" label="选择文件" class="source-file-field">
            <el-select v-model="chosen" filterable :disabled="busy" placeholder="选择之前上传的 TXT" style="width:100%"><el-option v-for="item in files" :key="item.id" :value="item.id" :label="`${item.name} · ${sizeText(item.bytes)} · ${item.rows ? item.rows.toLocaleString('zh-CN') + ' 条' : '待解析'}`" /></el-select><span class="field-note">复用源文件创建新任务，无需重复上传。</span>
          </el-form-item>
          <el-form-item v-else class="source-file-field" aria-label="TXT 文件">
            <el-upload drag :auto-upload="false" :limit="1" :disabled="busy" accept=".txt" :on-change="chooseFile" :on-remove="() => file = undefined" :on-exceed="() => error = '一次选择一个 TXT 文件，请先移除已选文件'">
              <el-icon class="el-icon--upload"><UploadFilled /></el-icon><strong>点击上传或将文件拖到这里</strong><span class="upload-hint">仅支持 .txt 文件，最大 8 GiB</span>
            </el-upload>
            <span class="field-note">UTF-8 编码，每行一条记录。大文件以原始字节流上传，不生成 multipart 临时副本。</span>
          </el-form-item>
          <el-form-item label="任务名称" for="task-name"><el-input id="task-name" v-model="name" placeholder="选填，默认使用文件名" maxlength="120" :disabled="busy" /><span class="field-note">名称重复时自动添加 -2、-3 后缀。</span></el-form-item>
          <div class="task-source-note"><el-icon><InfoFilled /></el-icon><span>每个分片最多 {{ unitRows.toLocaleString('zh-CN') }} 条记录，节点领包后使用全部线程处理。</span></div>
        </section>

        <div class="task-config-panels">
          <section class="execution-panel">
            <div class="task-section-heading"><h3><el-icon><Timer /></el-icon>请求策略</h3><span>仅对本任务生效</span></div>
            <div class="request-fields">
              <el-form-item label="账号最长验证 / 秒" for="task-timeout"><el-input-number id="task-timeout" v-model="timeout" :min="5" :max="120" :step="5" :precision="0" :disabled="busy" controls-position="right" /></el-form-item>
              <el-form-item label="时限内最多重试次数" for="task-retries"><el-input-number id="task-retries" v-model="retries" :min="0" :max="5" :step="1" :precision="0" :disabled="busy" controls-position="right" /></el-form-item>
            </div>
            <p class="config-hint">从拿到账号开始计时；超过时限立即记为超时并处理下一个账号。</p>
            <details class="request-help"><summary>超时与换 IP 说明</summary><p>仅在账号剩余时间内重试。超时、TLS、5xx 及 Apple 要求稍后再试时淘汰当前出口，优先取新 IP；可复用 IP 按使用次数最少轮转。</p></details>
          </section>
          <section class="device-policy-panel">
            <div class="task-section-heading"><h3><el-icon><Refresh /></el-icon>设备策略</h3><span class="task-scope-label">随任务保存</span></div>
            <div class="device-policy-fields">
              <div><el-form-item label="公共头刷新 / 个账号" for="task-public-header"><el-input-number id="task-public-header" v-model="publicHeaderUseLimit" :min="1" :max="10000" :precision="0" :disabled="busy" controls-position="right" /></el-form-item><p>每个线程达到阈值后<br />自动获取新公共头。</p></div>
              <div><el-form-item label="设备淘汰 / 个账号" for="task-device-limit"><el-input-number id="task-device-limit" v-model="deviceUseLimit" :min="1" :max="1000000" :step="100" :precision="0" :disabled="busy" controls-position="right" /></el-form-item><p>达到阈值后淘汰旧身份，<br />下一个账号自动重新注册。</p></div>
            </div>
            <div class="policy-scope-note">独立于其他任务，不会修改正在执行的任务。</div>
          </section>
        </div>
      </div>
      <el-alert v-if="error" class="create-task-error" :title="error" type="error" :closable="false" show-icon />
      <el-progress v-if="busy && uploadPercent !== null" class="upload-progress" :percentage="uploadPercent" :stroke-width="6" />
    </el-form>
    <template #footer><div class="create-dialog-footer"><span class="queue-note"><el-icon><InfoFilled /></el-icon>创建后加入队列，按顺序自动开始</span><div class="create-footer-actions"><el-button :disabled="busy" @click="createTaskOpen = false">取消</el-button><el-button type="primary" :loading="busy" native-type="submit" form="create-task-form">{{ busy ? (mode === 'reuse' ? '正在创建…' : `正在上传${uploadPercent == null ? '' : ` ${uploadPercent}%`}`) : '创建并加入队列' }}</el-button></div></div></template>
  </el-dialog>
</template>
<style>
.create-task-dialog.el-dialog{padding:0;border-radius:16px;overflow:hidden;max-height:88dvh;display:flex;flex-direction:column;box-shadow:0 24px 80px #162b3340}.create-task-dialog .el-dialog__header{padding:24px 28px 22px;margin:0;border-bottom:1px solid #e9eef1;flex-shrink:0}.create-task-dialog .el-dialog__headerbtn{top:20px;right:20px;width:32px;height:32px;border-radius:7px}.create-task-dialog .el-dialog__headerbtn:hover{background:#f2f5f7}.create-task-dialog .el-dialog__body{padding:0;overflow-y:auto;overscroll-behavior:contain}.create-task-dialog .el-dialog__footer{padding:18px 28px;border-top:1px solid #e7edf0;background:#fff;flex-shrink:0}.create-dialog-heading{display:flex;align-items:center;gap:13px;padding-right:32px}.create-dialog-icon{width:44px;height:44px;display:grid;place-items:center;border-radius:12px;background:#eaf7f0;color:#229b65;font-size:23px}.create-dialog-heading h2{margin:0;font-size:22px;font-weight:650;color:#283e49}.create-dialog-heading p{margin:5px 0 0;font-size:14px;color:#84919a;line-height:1.6}.create-task-form{padding:26px 28px}.create-task-columns{display:grid;grid-template-columns:minmax(0,.85fr) minmax(0,1.3fr);gap:26px}.source-panel{min-width:0;padding-right:26px;border-right:1px solid #e8eef0}.task-section-heading{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:18px}.task-section-heading h3{display:flex;align-items:center;gap:7px;font-size:16px;color:#304650;margin:0;font-weight:650}.task-section-heading h3 .el-icon{font-size:18px;color:#6c858f}.task-section-heading>span{font-size:12px;color:#8b98a1;white-space:nowrap}.source-toggle{display:flex;width:100%;margin-bottom:20px}.source-toggle .el-radio-button{flex:1}.source-toggle .el-radio-button__inner{width:100%;padding:10px 8px;font-size:13px}.source-file-field .el-form-item__content{display:block}.source-file-field .el-upload{display:block}.source-file-field .el-upload-dragger{padding:25px 12px;background:#f9fcfa;border:1px dashed #bcd9cc;border-radius:10px}.source-file-field .el-icon--upload{display:block;margin:0 auto 13px;font-size:36px;color:#56ad83}.source-file-field strong{display:block;color:#456052;font-size:14px;font-weight:500}.upload-hint{display:block;font-size:12px;color:#91a09a;margin-top:9px}.create-task-dialog .el-form-item{margin-bottom:18px}.create-task-dialog .el-form-item__label{font-size:14px!important;line-height:1.5;margin-bottom:10px}.create-task-dialog .field-note{font-size:12px;line-height:1.7;color:#8b99a2;margin-top:9px}.task-source-note{display:flex;gap:8px;align-items:flex-start;padding:12px;background:#f4f7f9;color:#82929c;border-radius:8px;font-size:12px;line-height:1.7}.task-source-note .el-icon{margin-top:3px;flex-shrink:0}.request-fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.request-fields .el-form-item{margin:0}.create-task-dialog .el-input-number{width:100%;min-width:0}.create-task-dialog .el-input-number.is-controls-right .el-input__wrapper{padding-left:8px;padding-right:32px}.config-hint{font-size:12px;line-height:1.7;color:#8a98a2;margin:13px 0 0}.request-help{font-size:12px;color:#6c838f;line-height:1.7;margin:10px 0 0}.request-help summary{cursor:pointer;width:fit-content}.request-help p{margin:8px 0 0}.device-policy-panel{margin-top:25px;border:1px solid #dfece4;border-radius:11px;padding:20px;background:#f7fbf8}.device-policy-panel .task-section-heading{margin-bottom:20px}.task-section-heading .task-scope-label{color:#398d63;background:#e8f4ec;padding:3px 7px;border-radius:5px;font-size:11px}.device-policy-fields{display:grid;grid-template-columns:1fr 1fr;gap:20px}.device-policy-fields .el-form-item{margin:0}.device-policy-fields p{font-size:12px;color:#86968c;line-height:1.7;margin:10px 0 0}.policy-scope-note{margin-top:17px;padding-top:12px;border-top:1px solid #e0eae3;font-size:12px;color:#6e8a78;line-height:1.6}.create-dialog-footer{display:flex;align-items:center;justify-content:space-between;gap:16px}.queue-note{display:flex;align-items:center;gap:6px;font-size:12px;color:#82959f}.create-footer-actions{display:flex;gap:10px}.create-footer-actions .el-button+.el-button{margin:0}.create-task-error{margin-top:20px}.upload-progress{margin-top:18px}
@media(max-width:900px){.create-task-dialog.el-dialog{width:calc(100vw - 24px)!important;margin-top:3dvh;max-height:94dvh}.create-task-dialog .el-dialog__header{padding:20px}.create-task-form{padding:20px}.create-task-columns{grid-template-columns:1fr;gap:24px}.source-panel{padding:0 0 22px;border-right:0;border-bottom:1px solid #e8eef0}.create-task-dialog .el-dialog__footer{padding:16px 20px}.queue-note{display:none}.create-footer-actions{width:100%}.create-footer-actions .el-button{flex:1}.create-footer-actions .el-button--primary{flex:2}.device-policy-panel{padding:17px}.device-policy-fields{gap:14px}.request-fields{gap:8px}.request-fields .el-form-item__label{font-size:12px!important}.device-policy-fields .el-form-item__label{font-size:13px!important}.create-dialog-heading h2{font-size:20px}.create-dialog-heading p{font-size:13px}.device-policy-panel{margin-top:22px}.create-task-dialog .el-dialog__headerbtn{right:12px;top:15px}}
</style>
