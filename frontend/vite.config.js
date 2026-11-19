import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const target = process.env.DASHBOARD_API || "http://localhost:8001";

export default defineConfig({
  plugins: [react()],
  build: {
    chunkSizeWarningLimit: 700,
    rollupOptions: {
      output: { manualChunks: { charts: ["recharts"], react: ["react", "react-dom"] } },
    },
  },
  server: {
    port: 3000,
    proxy: {
      "/api": target,
      "/ws": { target: target.replace("http", "ws"), ws: true },
    },
  },
});
