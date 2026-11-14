<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { api, fmt } from '../composables/useCluster'

const props = defineProps<{ job: any }>()
const pageSize = 100
const category = ref('all')
const offset = ref(0)
const rows = ref<any[]>([])
const total = ref(0)
const loading = ref(false)
const error = ref('')
let generation = 0
let foregroundRequests = 0
let backgroundPending = false
let backgroundQueued = false

const options = [
  { label: '全部结果', value: 'all' },
  { label: '密正', value: 'valid' },
  { label: '密码错误', value: 'wrong_password' },
  { label: '双重验证', value: 'need_2fa' },
  { label: '锁定', value: 'locked' },
  { label: '已失效', value: 'invalid' },
  { label: '超时', value: 'timeout' },
  { label: '其他未验证', value: 'undetermined' },
]
const labels: Record<string, string> = Object.fromEntries(options.map(item => [item.value, item.label]))
const valid = computed(() => category.value === 'valid')
const visibleTotal = computed(() => valid.value ? total.value : Math.min(total.value, pageSize))
const page = computed(() => Math.floor(offset.value / pageSize) + 1)
const pageCount = computed(() => Math.max(1, Math.ceil(total.value / pageSize)))

async function load(reset = false, showLoading = true) {
  if (!props.job?.id) return
  if (reset || !valid.value) offset.value = 0
  const current = ++generation
  if (showLoading) {
    foregroundRequests += 1
    loading.value = true
    error.value = ''
  }
  try {
    const result = await api(`/api/jobs/${props.job.id}/rows?category=${category.value}&offset=${offset.value}&limit=${pageSize}`)
    if (current !== generation) return
    rows.value = Array.isArray(result.rows) ? result.rows : []
    total.value = Math.max(0, Number(result.total) || 0)
  } catch (exc: any) {
    if (current === generation) error.value = exc?.message || '结果加载失败'
  } finally {
    if (showLoading) {
      foregroundRequests = Math.max(0, foregroundRequests - 1)
      loading.value = foregroundRequests > 0
    }
  }
}
async function refreshInBackground() {
  if (backgroundPending) {
    backgroundQueued = true
    return
  }
  backgroundPending = true
  try {
    do {
      backgroundQueued = false
      await load(false, false)
    } while (backgroundQueued)
  } finally {
    backgroundPending = false
  }
}
function changeCategory() { load(true) }
function previous() { offset.value = Math.max(0, offset.value - pageSize); load() }
function next() { offset.value += pageSize; load() }
const elapsed = (value: any) => value == null ? '—' : `${Number(value).toFixed(2)}s`
const display = (value: any) => value == null || value === '' ? '—' : value
const gsaCode = (row: any) => display(row.gsa_code ?? (row.protocol === 'GSA' ? row.code : null))

watch(
  () => [props.job?.id, Number(props.job?.confirmed) || 0] as const,
  ([jobId, confirmed], previous) => {
    if (!jobId) return
    if (!previous || previous[0] !== jobId) {
      void load(true, true)
      return
    }
    if (confirmed !== previous[1]) void refreshInBackground()
  },
  { immediate: true },
)
</script>

<template>
  <el-card shadow="never" class="section-card result-list-card">
    <template #header>
      <div class="section-heading result-list-heading">
        <div><h2>账号结果</h2><p>密正结果可逐页查看全部；其他分类各显示前 100 条。</p></div>
        <el-select v-model="category" class="result-filter" @change="changeCategory">
          <el-option v-for="item in options" :key="item.value" :label="item.label" :value="item.value" />
        </el-select>
      </div>
    </template>
    <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon />
    <el-table v-loading="loading" :data="rows" empty-text="当前分类暂无已确认结果" stripe>
      <el-table-column prop="row" label="#" width="90" />
      <el-table-column prop="account" label="账号" min-width="260" show-overflow-tooltip />
      <el-table-column label="状态" width="120"><template #default>{{ labels[category] }}</template></el-table-column>
      <el-table-column prop="protocol" label="协议" min-width="150" show-overflow-tooltip />
      <el-table-column prop="stage" label="阶段" min-width="130" show-overflow-tooltip />
      <el-table-column prop="http" label="HTTP" width="90" />
      <el-table-column prop="code" label="结果码" min-width="140" show-overflow-tooltip />
      <el-table-column label="GSA 码" min-width="130"><template #default="scope">{{ gsaCode(scope.row) }}</template></el-table-column>
      <el-table-column label="网页码" min-width="130"><template #default="scope">{{ display(scope.row.web_code) }}</template></el-table-column>
      <el-table-column prop="device" label="设备" width="90" />
      <el-table-column label="耗时" width="100"><template #default="scope">{{ elapsed(scope.row.elapsed) }}</template></el-table-column>
      <el-table-column prop="attempts" label="重试" width="80" />
    </el-table>
    <div class="result-list-footer">
      <span v-if="valid">共 {{ fmt(total) }} 条密正结果 · 第 {{ page }} / {{ pageCount }} 页</span>
      <span v-else-if="category === 'all'">共 {{ fmt(total) }} 条已确认结果，当前显示前 {{ fmt(visibleTotal) }} 条</span>
      <span v-else>共 {{ fmt(total) }} 条{{ labels[category] }}，当前显示前 {{ fmt(visibleTotal) }} 条</span>
      <div v-if="valid && total > pageSize" class="actions">
        <el-button :disabled="offset === 0 || loading" @click="previous">上一页</el-button>
        <el-button :disabled="offset + pageSize >= total || loading" @click="next">下一页</el-button>
      </div>
    </div>
    <p class="result-list-note">列表只显示已回传并由总控确认的结果；正在处理的工作包完成后会自动出现。
    </p>
  </el-card>
</template>
