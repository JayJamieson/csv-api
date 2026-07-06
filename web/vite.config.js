import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// The Go API's default port is 8001 (cmd/server/main.go). Override with
// GO_API_URL when running the server on a different port.
const apiTarget = process.env.GO_API_URL || "http://localhost:8001";

// The Go server serves the built SPA from web/dist at its root, so the app's
// fetches are same-origin in production. In dev, Vite proxies the API paths to
// the Go server so the same relative-URL fetches work.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      "/load": { target: apiTarget, changeOrigin: true },
      "/imports": { target: apiTarget, changeOrigin: true },
      "/api": { target: apiTarget, changeOrigin: true },
      "/import": { target: apiTarget, changeOrigin: true },
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
