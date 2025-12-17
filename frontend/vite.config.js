import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const isProd = mode === 'production'
  return {
    base: isProd ? '/ai_watch_stock/' : '/',
    plugins: [react()],
    build: {
      outDir: 'ai_watch_stock' // 这里填你想要的文件夹名
    }
  }
})
