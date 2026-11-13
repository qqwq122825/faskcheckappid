<script setup lang="ts">
defineProps<{ label: string, value: string | number, unit?: string, note?: string, accent?: boolean }>()
</script>
<template><el-card shadow="never" class="metric-card" :class="{ accent }"><span class="metric-label">{{ label }}</span><div class="metric-value">{{ value }}<small v-if="unit">{{ unit }}</small></div><p v-if="note">{{ note }}</p></el-card></template>
