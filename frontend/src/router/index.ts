import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'
import DedupGroupView from '@/views/DedupGroupView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 去重组对照视图：比通用工作台多一层组内对照与重放入口。
    { path: '/dedup-groups', name: 'dedup-groups', component: DedupGroupView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
