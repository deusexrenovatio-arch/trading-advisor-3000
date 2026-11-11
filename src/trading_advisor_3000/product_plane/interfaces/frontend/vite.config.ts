import { defineConfig } from "vite";

export default defineConfig({
  base: "/journal/",
  server: {
    host: "127.0.0.1",
    proxy: {
      "/api": "http://127.0.0.1:8107",
      "/ready": "http://127.0.0.1:8107",
    },
  },
  build: { target: "es2022" },
});
