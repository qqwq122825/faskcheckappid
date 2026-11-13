import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  base: '/static/ui/',
  build: { outDir: '../clusterlab/static/ui', emptyOutDir: true },
  server: { proxy: { '/api': 'http://127.0.0.1:18770' } },
})
