import { build } from "esbuild";

await build({
  entryPoints: {
    "test-indexdb-glide": "src/test-indexdb-glide.jsx",
  },
  bundle: true,
  format: "iife",
  target: ["es2020"],
  outdir: "www",
  jsx: "automatic",
  sourcemap: true,
});
