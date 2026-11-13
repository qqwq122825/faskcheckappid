<script setup lang="ts">
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { House, List, Connection, Document, Setting, Fold, Expand, SwitchButton, Grid } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import { session, state, isNode, connected, bootError, lastUpdated, initialize, stopPolling, login, logout } from './composables/useCluster'
import CreateTaskDialog from './components/CreateTaskDialog.vue'
const route = useRoute(), router = useRouter(), collapsed = ref(false), loggingIn = ref(false), loginError = ref('')
const credentials = reactive({ username: 'admin', password: '' })
const menu = computed(() => isNode.value ? [
  { path: '/', label: '总控连接', icon: Connection }, { path: '/jobs', label: '本机工作包', icon: List }, { path: '/events', label: '连接状态', icon: Document }, { path: '/settings', label: '系统信息', icon: Setting },
] : [
  { path: '/', label: '首页', icon: House }, { path: '/jobs', label: '任务管理', icon: List }, { path: '/nodes', label: '节点配置', icon: Connection }, { path: '/events', label: '运行日志', icon: Document }, { path: '/settings', label: '系统信息', icon: Setting },
])
const title = computed(() => isNode.value && route.path === '/' ? '节点连接' : isNode.value && route.path === '/jobs' ? '本机工作包' : route.meta.title)
const active = computed(() => route.path.startsWith('/jobs') ? '/jobs' : route.path)
async function submitLogin() {
  loggingIn.value = true; loginError.value = ''
  try { await login(credentials.username, credentials.password); credentials.password = '' }
  catch (e) { loginError.value = (e as Error).message }
  finally { loggingIn.value = false }
}
async function signOut() { try { await logout() } catch (e) { ElMessage.error((e as Error).message) } }
onMounted(initialize)
onUnmounted(stopPolling)
</script>
<template>
  <div v-if="!session" class="boot-screen"><el-result v-if="bootError" icon="error" title="无法连接本地服务" :sub-title="bootError"><template #extra><el-button type="primary" @click="initialize">重新连接</el-button></template></el-result><div v-else>正在连接管理服务…</div></div>
  <div v-else-if="!session.authenticated" class="login-screen"><el-card class="login-box" shadow="never"><div class="login-logo"><el-icon><Grid /></el-icon>Cluster Lab</div><h1>登录管理后台</h1><p class="muted">{{ isNode ? '执行节点' : '集群总控' }}</p><el-form label-position="top" @submit.prevent="submitLogin"><el-form-item label="用户名"><el-input v-model="credentials.username" autocomplete="username" /></el-form-item><el-form-item label="管理密码"><el-input v-model="credentials.password" type="password" show-password autocomplete="current-password" /></el-form-item><el-alert v-if="loginError" :title="loginError" type="error" :closable="false" /><el-button type="primary" native-type="submit" :loading="loggingIn">登录</el-button></el-form></el-card></div>
  <div v-else class="admin-layout" :class="{ collapsed }">
    <aside class="sidebar"><a class="brand" href="#/"><span class="brand-icon"><el-icon><Grid /></el-icon></span><span v-if="!collapsed">Cluster <b>Lab</b><small>{{ isNode ? '节点管理' : '集群管理后台' }}</small></span></a><div v-if="!collapsed" class="sidebar-section">{{ isNode ? '本机工作空间' : '管理工作空间' }}</div><el-menu :default-active="active" :collapse="collapsed" :collapse-transition="false" background-color="transparent" text-color="#cbd2d6" active-text-color="#fff" @select="(path: string) => router.push(path)"><el-menu-item v-for="item in menu" :key="item.path" :index="item.path"><el-icon><component :is="item.icon" /></el-icon><template #title>{{ item.label }}</template></el-menu-item></el-menu><div v-if="!collapsed" class="sidebar-bottom"><span class="sidebar-status"><i />独立离线任务平台</span><span>v{{ session.version }}</span></div></aside>
    <div class="main-layout"><header class="topbar"><el-button text :icon="collapsed ? Expand : Fold" :aria-label="collapsed ? '展开菜单' : '收起菜单'" @click="collapsed = !collapsed" /><el-breadcrumb separator="/"><el-breadcrumb-item @click="router.push('/')">{{ isNode ? '执行节点' : '集群管理' }}</el-breadcrumb-item><el-breadcrumb-item>{{ title }}</el-breadcrumb-item></el-breadcrumb><div class="topbar-right"><span class="connection-state" :class="{ disconnected: !connected }"><i />{{ connected ? '服务已连接' : state ? '连接中断' : '正在连接' }}</span><span class="admin-label">管理员</span><el-button v-if="session.password_required" text :icon="SwitchButton" @click="signOut">退出</el-button></div></header>
      <main class="page"><div class="page-heading"><div><h1>{{ title }}</h1><p>{{ isNode && route.path === '/' ? '部署时连接一次，后续通过总控统一管理。' : route.meta.description }}</p></div><span v-if="lastUpdated" class="update-time">更新于 {{ new Date(lastUpdated).toLocaleTimeString('zh-CN', { hour12: false }) }}</span></div><el-alert v-if="!connected && state" title="与本机服务连接中断，当前显示上次成功获取的数据，正在重试。" type="warning" :closable="false" show-icon /><div v-if="!state" v-loading="true" class="loading-state" /><router-view v-else /><footer class="page-footer">Cluster Lab · 离线任务平台<span>Vue + Python</span></footer></main>
    </div><CreateTaskDialog v-if="!isNode" />
  </div>
</template>
