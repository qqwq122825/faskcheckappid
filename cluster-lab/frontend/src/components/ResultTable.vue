<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { api, fmt } from '../composables/useCluster'

const props = defineProps<{ jobId: string, rows?: number, status?: string, exportable?: boolean }>()
// 记录量大时不渲染表格：逐行翻页对百万级以上没有实际意义，
// 也避免前端一次拉取几十万行的滚动卡顿。明细走导出。
const TABLE_ROW_LIMIT = 100000
const showTable = computed(() => (props.rows ?? 0) <= TABLE_ROW_LIMIT)
const categories = [
  { value: 'all', label: '全部结果' },
  { value: 'valid', label: '密正' },
  { value: 'need_2fa', label: '双重验证' },
  { value: 'wrong_password', label: '密码错误' },
  { value: 'locked', label: '锁定' },
  { value: 'timeout', label: '超时' },
  { value: 'unknown', label: '未判断' },
]
const category = ref('all')
const offset = ref(0)
const limit = 50
const rows = ref<any[]>([])
const total = ref(0)
const loading = ref(false)
const error = ref('')

async function load() {
  if (!props.jobId) return
  loading.value = true; error.value = ''
  try {
    const data = await api(`/api/jobs/${props.jobId}/rows?offset=${offset.value}&limit=${limit}&category=${category.value}`)
    rows.value = data.rows || []
    total.value = data.total || 0
  } catch (exc: any) {
    error.value = exc?.message || '读取结果失败'
  } finally { loading.value = false }
}
watch([() => props.jobId, category], () => { offset.value = 0; load() })
watch(() => props.status, (value, before) => { if (value !== before) load() })
load()

const page = computed(() => Math.floor(offset.value / limit) + 1)
function changePage(next: number) {
  offset.value = Math.max(0, (next - 1) * limit)
  load()
}
function label(status: string) {
  return categories.find(c => c.value === status)?.label || status || '—'
}
function tone(status: string) {
  return status === 'valid' ? 'success' : status === 'locked' ? 'danger'
    : status === 'timeout' ? 'warning' : status === 'need_2fa' ? 'warning' : 'info'
}
</script>

<template>
  <el-card shadow="never" class="section-card">
    <template #header>
      <div class="section-heading">
        <div><h2>处理结果</h2><p>按行号顺序展示已确认记录；筛选只影响当前视图，不改变任务状态。</p></div>
        <div class="result-tools">
          <el-select v-model="category" size="default" style="width:170px">
            <el-option v-for="c in categories" :key="c.value" :label="c.label" :value="c.value" />
          </el-select>
          <el-button v-if="exportable" tag="a" :href="`/api/jobs/${jobId}/results`">导出记录</el-button>
        </div>
      </div>
    </template>
    <el-table v-if="showTable" :data="rows" v-loading="loading" size="default" style="width:100%">
      <el-table-column label="#" width="90"><template #default="{ row }">{{ fmt(row.row) }}</template></el-table-column>
      <el-table-column label="账号" min-width="320" show-overflow-tooltip>
        <template #default="{ row }">{{ row.account || '—' }}</template>
      </el-table-column>
      <el-table-column label="状态" width="130">
        <template #default="{ row }"><el-tag :type="tone(row.status)" effect="light">{{ label(row.status) }}</el-tag></template>
      </el-table-column>
      <el-table-column label="阶段" width="120"><template #default="{ row }">{{ row.stage || '—' }}</template></el-table-column>
      <el-table-column label="HTTP" width="90"><template #default="{ row }">{{ row.http ?? '—' }}</template></el-table-column>
      <el-table-column label="业务码" width="110"><template #default="{ row }">{{ row.code ?? '—' }}</template></el-table-column>
      <el-table-column label="设备" width="90"><template #default="{ row }">{{ row.device ?? '—' }}</template></el-table-column>
      <el-table-column label="耗时" width="110"><template #default="{ row }">{{ row.elapsed != null ? row.elapsed + 's' : '—' }}</template></el-table-column>
      <el-table-column label="重试" width="90"><template #default="{ row }">{{ row.attempts ?? 0 }}</template></el-table-column>
      <template #empty><span class="muted">{{ error || (loading ? '读取中…' : '暂无已确认记录') }}</span></template>
    </el-table>
    <div v-if="showTable" class="result-pager">
      <span>{{ fmt(total) }} 条结果 · 第 {{ page }} 页</span>
      <div>
        <el-button :disabled="offset === 0" @click="changePage(page - 1)">上一页</el-button>
        <el-button :disabled="offset + limit >= total" @click="changePage(page + 1)">下一页</el-button>
      </div>
    </div>
    <div v-else class="result-large">
      <p>共 {{ fmt(props.rows) }} 条记录，超过 {{ fmt(TABLE_ROW_LIMIT) }} 条不再逐行展示。</p>
      <p class="muted">请按分类导出 TXT 查看明细；上方统计仍然实时更新。</p>
    </div>
  </el-card>
</template>
