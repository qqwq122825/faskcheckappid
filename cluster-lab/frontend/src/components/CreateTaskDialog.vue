<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { UploadFilled } from '@element-plus/icons-vue'
import { api, busy, createTaskOpen, refresh, state } from '../composables/useCluster'
const router = useRouter()
const name = ref(''), unitRows = ref(1000), file = ref<File>(), error = ref(''), advanced = ref<string[]>([])
const files = ref<any[]>([]), chosen = ref(''), mode = ref<'new' | 'reuse'>('new')
const maxUnitRows = computed(() => state.value?.settings?.max_unit_rows || 10000)
watch(createTaskOpen, open => {
  if (open) {
    error.value = ''; file.value = undefined; name.value = ''
    unitRows.value = Math.min(10000, maxUnitRows.value); advanced.value = []
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
async function submit() {
  error.value = ''
  if (mode.value === 'reuse') {
    if (!chosen.value) { error.value = '请选择一个已上传的文件'; return }
    if (!Number.isInteger(unitRows.value) || unitRows.value < 50 || unitRows.value > maxUnitRows.value) {
      error.value = `每个分片应为 50–${maxUnitRows.value.toLocaleString('zh-CN')} 条`; return
    }
    busy.value = true
    try {
      await api('/api/jobs/from-file', { source: chosen.value, name: name.value.trim(),
                                         unit_rows: unitRows.value, mode: 'auth', enqueue: 'true' }, 'POST')
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
    const data = new FormData()
    data.append('file', file.value)
    data.append('name', name.value.trim() || file.value.name)
    data.append('unit_rows', String(unitRows.value))
    data.append('mode', 'auth')
    data.append('enqueue', 'true')
    await api('/api/jobs', data)
    createTaskOpen.value = false
    await refresh()
    await router.push('/jobs')
  } catch (e) { error.value = (e as Error).message }
  finally { busy.value = false }
}
</script>
<template>
  <el-dialog v-model="createTaskOpen" title="上传 TXT 创建任务" width="600px" :close-on-click-modal="!busy" :close-on-press-escape="!busy" :show-close="!busy" destroy-on-close>
    <el-form label-position="top" @submit.prevent="submit">
      <el-form-item label="输入来源">
        <el-radio-group v-model="mode" :disabled="busy">
          <el-radio-button value="new">上传新文件</el-radio-button>
          <el-radio-button value="reuse" :disabled="!files.length">使用已上传文件</el-radio-button>
        </el-radio-group>
      </el-form-item>
      <el-form-item v-if="mode === 'reuse'" label="选择文件">
        <el-select v-model="chosen" filterable style="width:100%" :disabled="busy"
                   placeholder="选择之前上传的 TXT">
          <el-option v-for="item in files" :key="item.id" :value="item.id"
                     :label="`${item.name} · ${sizeText(item.bytes)} · ${item.rows ? item.rows.toLocaleString('zh-CN') + ' 条' : '待解析'}`" />
        </el-select>
        <span class="field-note">复用同一份源文件再建任务，不用重复上传；分片大小可以重新选。</span>
      </el-form-item>
      <el-form-item v-if="mode === 'new'" label="TXT 文件">
        <el-upload drag :auto-upload="false" :limit="1" :disabled="busy" accept=".txt" :on-change="chooseFile" :on-remove="() => file = undefined" :on-exceed="() => error = '一次选择一个 TXT 文件，请先移除已选文件'">
          <el-icon class="el-icon--upload"><UploadFilled /></el-icon><div>点击选择或拖入 TXT 文件</div>
          <template #tip><div class="el-upload__tip">UTF-8 编码，每行一条记录，最大 2 GiB。</div></template>
        </el-upload>
      </el-form-item>
      <el-form-item label="任务名称（可选）"><el-input v-model="name" placeholder="不填写则使用上传的文件名；重名会自动加 -2、-3" maxlength="120" :disabled="busy" /></el-form-item>
      <el-collapse v-model="advanced" class="create-advanced"><el-collapse-item title="高级设置（通常无需调整）" name="advanced"><el-form-item label="每个分片的记录数"><el-input-number v-model="unitRows" :min="50" :max="maxUnitRows" :step="50" :precision="0" :disabled="busy" /><span class="field-note">总控上限 {{ maxUnitRows.toLocaleString('zh-CN') }} 条；默认 {{ Math.min(10000, maxUnitRows).toLocaleString('zh-CN') }} 条，同时受 32 MiB 字节上限约束。</span></el-form-item></el-collapse-item></el-collapse>
      <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon />
      <p class="muted">创建后自动加入执行队列；前一个任务全部确认后，轮到本任务自动开始。</p>
    </el-form>
    <template #footer><el-button :disabled="busy" @click="createTaskOpen = false">取消</el-button><el-button type="primary" :loading="busy" @click="submit">{{ busy ? '正在上传并创建…' : '创建并加入队列' }}</el-button></template>
  </el-dialog>
</template>
