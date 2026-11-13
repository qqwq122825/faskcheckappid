<script setup lang="ts">
import { ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { UploadFilled } from '@element-plus/icons-vue'
import { api, busy, createTaskOpen, refresh } from '../composables/useCluster'
const router = useRouter()
const name = ref(''), unitRows = ref(1000), file = ref<File>(), error = ref(''), advanced = ref<string[]>([])
watch(createTaskOpen, open => {
  if (open) { error.value = ''; file.value = undefined; name.value = ''; unitRows.value = 1000; advanced.value = [] }
})
function chooseFile(upload: { raw?: File }) {
  file.value = upload.raw
  error.value = ''
  if (file.value && !file.value.name.toLowerCase().endsWith('.txt')) {
    file.value = undefined
    error.value = '请选择 .txt 文本文件'
  }
}
async function submit() {
  error.value = ''
  if (!file.value) { error.value = '请先选择 TXT 文件'; return }
  busy.value = true
  try {
    const data = new FormData()
    data.append('file', file.value)
    data.append('name', name.value.trim() || file.value.name)
    data.append('unit_rows', String(unitRows.value))
    await api('/api/jobs', data)
    createTaskOpen.value = false
    await refresh()
    await router.push('/')
  } catch (e) { error.value = (e as Error).message }
  finally { busy.value = false }
}
</script>
<template>
  <el-dialog v-model="createTaskOpen" title="上传 TXT 创建任务" width="600px" :close-on-click-modal="!busy" :close-on-press-escape="!busy" :show-close="!busy" destroy-on-close>
    <el-form label-position="top" @submit.prevent="submit">
      <el-form-item label="TXT 文件">
        <el-upload drag :auto-upload="false" :limit="1" :disabled="busy" accept=".txt" :on-change="chooseFile" :on-remove="() => file = undefined" :on-exceed="() => error = '一次选择一个 TXT 文件，请先移除已选文件'">
          <el-icon class="el-icon--upload"><UploadFilled /></el-icon><div>点击选择或拖入 TXT 文件</div>
          <template #tip><div class="el-upload__tip">UTF-8 编码，每行一条记录，最大 2 GiB。</div></template>
        </el-upload>
      </el-form-item>
      <el-form-item label="任务名称（可选）"><el-input v-model="name" placeholder="不填写则使用文件名" maxlength="120" :disabled="busy" /></el-form-item>
      <el-collapse v-model="advanced" class="create-advanced"><el-collapse-item title="高级设置（通常无需调整）" name="advanced"><el-form-item label="每个工作包的记录数"><el-input-number v-model="unitRows" :min="50" :max="100000" :step="50" :disabled="busy" /><span class="field-note">默认 1,000 条，同时受 4 MiB 字节上限约束。</span></el-form-item></el-collapse-item></el-collapse>
      <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon />
      <p class="muted">创建后回到首页查看进度，文件准备完成即可开始任务。</p>
    </el-form>
    <template #footer><el-button :disabled="busy" @click="createTaskOpen = false">取消</el-button><el-button type="primary" :loading="busy" @click="submit">{{ busy ? '正在上传并创建…' : '创建任务' }}</el-button></template>
  </el-dialog>
</template>
