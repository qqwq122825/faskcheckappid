<script setup lang="ts">
import { computed, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { api, fmt, state } from '../composables/useCluster'

const visible = ref(false), nodeId = ref(''), capacity = ref(2), error = ref('')
// 每次刷新都从全局状态里取最新节点，设备池进度才会跟着心跳走
const node = computed(() => state.value?.nodes?.find((n: any) => n.id === nodeId.value))
const busy = ref(false), timer = 0 as any

// 设备池状态来自节点的心跳上报，总控只是展示
const pool = computed(() => node.value?.telemetry?.device_pool || {})
const building = computed(() => Boolean(pool.value.running))
const ready = computed(() => pool.value.ready || 0)
const actual = computed(() => (pool.value.built || 0) + (pool.value.failed || 0))
const rows = computed(() => {
  const reported = pool.value.slots
  if (Array.isArray(reported) && reported.length) {
    return reported.map((item: any) => ({
      slot: item.slot,
      state: item.state === 'ready' ? '已就绪' : item.state === 'pending' ? '待注册' : '未开始',
      proxy: item.proxy || '—',
      at: item.at || '—',
    }))
  }
  const total = Math.max(pool.value.capacity || 0, capacity.value)
  return Array.from({ length: total }, (_, index) => ({
    slot: index + 1,
    state: index < ready.value ? '已就绪' : index < actual.value ? '已尝试' : '未开始',
    proxy: '—', at: '—',
  }))
})

function show(value: any) {
  nodeId.value = value?.id || ''
  capacity.value = Math.max(1, Number(value?.telemetry?.device_pool?.capacity) || 2)
  error.value = ''
  visible.value = true
}
defineExpose({ show })

async function initialize() {
  error.value = ''
  if (!node.value?.id) { error.value = '节点不可用'; return }
  if (!Number.isInteger(capacity.value) || capacity.value < 1 || capacity.value > 200) {
    error.value = '设备数量应为 1–200 的整数'; return
  }
  busy.value = true
  try {
    await api(`/api/nodes/${node.value.id}/devices`, { capacity: capacity.value }, 'POST')
    ElMessage.success(`已请求节点初始化 ${capacity.value} 台设备`)
  } catch (exc: any) {
    error.value = exc?.message || '请求失败'
  } finally { busy.value = false }
}
async function cancelBuild() {
  try { await api(`/api/nodes/${node.value.id}/devices`, { capacity: 0 }, 'POST'); ElMessage.success('已请求停止') }
  catch (exc: any) { error.value = exc?.message || '请求失败' }
}
</script>

<template>
  <el-drawer v-model="visible" size="640px" title="设备池" class="device-pool-drawer">
    <el-alert v-if="!node?.telemetry" type="info" :closable="false" show-icon
              title="节点未上报状态，无法读取设备池。" />
    <section class="device-pool-target">
      <div class="section-heading"><h2>初始化设备</h2>
        <el-tag :type="building ? 'warning' : ready ? 'success' : 'info'">
          {{ building ? '初始化中' : ready ? '已就绪' : '未开始' }}</el-tag>
      </div>
      <el-form label-position="top" @submit.prevent="initialize">
        <el-form-item label="设备数量">
          <el-input-number v-model="capacity" :min="1" :max="200" :step="1" :precision="0" :disabled="building || busy" />
          <span class="field-note">设备在节点本地注册，每台约 6–8 秒；不占用任务隧道。</span>
        </el-form-item>
        <div class="actions">
          <el-button type="primary" native-type="submit" :loading="busy" :disabled="building">初始化设备</el-button>
          <el-button v-if="building" @click="cancelBuild">停止</el-button>
        </div>
        <el-alert v-if="error" :title="error" type="error" :closable="false" show-icon />
      </el-form>
    </section>
    <div class="device-pool-summary">
      <div><span>目标数量</span><strong>{{ fmt(pool.capacity || capacity) }}</strong></div>
      <div><span>已尝试</span><strong>{{ fmt(actual) }}</strong></div>
      <div><span>就绪设备</span><strong>{{ fmt(ready) }}</strong></div>
    </div>
    <section>
      <div class="section-heading device-list-heading"><h2>设备列表</h2>
        <span class="muted">{{ building ? `正在注册第 ${pool.built + pool.failed + 1} 台` : '等待中' }}</span>
      </div>
      <el-table :data="rows" empty-text="尚未初始化设备" stripe max-height="360">
        <el-table-column prop="slot" label="设备编号" width="100" />
        <el-table-column prop="state" label="状态" width="110" />
        <el-table-column prop="proxy" label="注册出口 IP" min-width="160" show-overflow-tooltip />
        <el-table-column prop="at" label="注册时间" width="120" />
      </el-table>
    </section>
  </el-drawer>
</template>
