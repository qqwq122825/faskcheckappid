<script setup lang="ts">
import { date, fmt } from '../composables/useCluster'

defineProps<{ feedback: any }>()
defineEmits<{ (event: 'open-nodes'): void }>()
</script>

<template>
  <el-card v-if="feedback" shadow="never" class="runtime-feedback section-card">
    <div class="runtime-feedback-head">
      <div>
        <div class="runtime-feedback-title">
          <el-tag :type="feedback.type" effect="light" round>{{ feedback.phase }}</el-tag>
          <h2>{{ feedback.title }}</h2>
        </div>
        <p>{{ feedback.description }}</p>
      </div>
      <el-button v-if="feedback.action === 'nodes'" type="primary" plain @click="$emit('open-nodes')">查看节点配置</el-button>
    </div>
    <el-steps :active="feedback.step" simple finish-status="success" process-status="process" class="runtime-steps">
      <el-step title="任务已创建" />
      <el-step title="等待节点" />
      <el-step title="节点执行" />
      <el-step title="结果确认" />
    </el-steps>
    <div class="runtime-feedback-meta">
      <span>在线节点 <strong>{{ fmt(feedback.online) }}</strong></span>
      <span>可接单 <strong>{{ fmt(feedback.accepting) }}</strong></span>
      <span>在途工作包 <strong>{{ fmt(feedback.active) }}</strong></span>
      <span>待分配工作包 <strong>{{ fmt(feedback.pending) }}</strong></span>
      <span>最近节点心跳 <strong>{{ feedback.latestHeartbeat ? date(feedback.latestHeartbeat) : '暂无' }}</strong></span>
    </div>
  </el-card>
</template>
