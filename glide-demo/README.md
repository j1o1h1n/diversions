# Glide Demo

This folder is a source-first standalone demo of:

- a React page using Glide Data Grid
- a service worker writing fake order data into IndexedDB
- BroadcastChannel notifications telling the page to reload from IndexedDB

It is intended to be copied elsewhere and rebuilt there.

## Files

- `package.json`: demo dependencies and build script
- `package-lock.json`: locked dependency versions
- `build.mjs`: esbuild config
- `src/test-indexdb-glide.jsx`: React + Glide page
- `www/test-indexdb-glide.html`: page shell
- `www/test-indexdb-glide-sw.js`: service worker

The built assets `www/test-indexdb-glide.js` and `www/test-indexdb-glide.css` are not checked in here. Generate them locally with the build step below.

## Build

1. Install dependencies:

```bash
npm install --legacy-peer-deps
```

2. Build the browser assets:

```bash
npm run build
```

This generates:

- `www/test-indexdb-glide.js`
- `www/test-indexdb-glide.js.map`
- `www/test-indexdb-glide.css`
- `www/test-indexdb-glide.css.map`

## Run

Serve the `www/` folder from a local web server. Do not open the HTML file directly from disk because service workers require an HTTP origin.

For example:

```bash
npx serve www
```

Then open:

- `http://localhost:3000/test-indexdb-glide.html`

The page will:

- register `test-indexdb-glide-sw.js`
- let you add 10,000 fake orders per click
- store them in IndexedDB
- broadcast updates
- reload the Glide grid from IndexedDB

## Notes

- The page and service worker use relative paths so the demo can be moved to another project without assuming a `/static/` URL prefix.
- The service worker imports `idb` from a CDN.
- The object store is keyed by `orderid` and includes indexes on `orderid`, `client`, and `exchange`.
