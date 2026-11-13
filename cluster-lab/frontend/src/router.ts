import { createRouter, createWebHashHistory } from 'vue-router'

export const router = createRouter({
  history: createWebHashHistory(),
  scrollBehavior: () => ({ top: 0 }),
  routes: [
    { path: '/', component: () => import('./views/Overview.vue'), meta: { title: '首页', description: '直接查看当前任务的进度、结果与处理速度。' } },
    { path: '/jobs', component: () => import('./views/Jobs.vue'), meta: { title: '任务管理', description: '创建任务、查看进度，按任务下载结果。' } },
    { path: '/jobs/:id', component: () => import('./views/JobDetail.vue'), meta: { title: '任务详情', description: '本页统计仅属于当前任务。' } },
    { path: '/nodes', component: () => import('./views/Nodes.vue'), meta: { title: '节点配置', description: '集中查看节点状态，调整后续工作包的执行配置。' } },
    { path: '/events', component: () => import('./views/Events.vue'), meta: { title: '运行日志', description: '查看任务操作、节点连接与结果确认记录。' } },
    { path: '/settings', component: () => import('./views/Settings.vue'), meta: { title: '系统设置', description: '配置新任务分片上限，查看当前部署信息。' } },
    { path: '/:pathMatch(.*)*', redirect: '/' },
  ],
})
