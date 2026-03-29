# IndexedDB Notes

This file explains the small IndexedDB + service worker demo in this repo and the pattern it is meant to teach.

## Goal

The goal is to understand a multi-window PWA data flow where:

1. A page tells the service worker that new data is available.
2. The service worker writes that data into IndexedDB.
3. The service worker broadcasts a message saying the data changed.
4. Any open window hears that message and reloads from IndexedDB.

This is useful because:

- IndexedDB is the durable shared store.
- `BroadcastChannel` is the live notification mechanism.
- The service worker can act as the shared writer/router.

## Demo Files

The demo lives in two files:

- [www/test-indexdb.html](/Users/johnl/dev/work/viewserver/www/test-indexdb.html)
- [www/test-indexdb-sw.js](/Users/johnl/dev/work/viewserver/www/test-indexdb-sw.js)

Open the page at:

```text
/static/test-indexdb.html
```

## Demo Flow

The demo works like this:

1. Click the button in `test-indexdb.html`.
2. The page sends `postMessage({ type: "add-row" })` to the service worker.
3. The service worker receives that message.
4. The service worker opens IndexedDB and writes one row.
5. The service worker broadcasts a `row-added` message on `BroadcastChannel("test-indexdb")`.
6. The page listens on that broadcast channel.
7. When the page hears `row-added`, it re-reads IndexedDB and redraws the HTML table.

The important design point is:

- the broadcast is not the data
- the broadcast is just the notification that the data changed
- IndexedDB is the source of truth

## Why Use BroadcastChannel

`BroadcastChannel` is a good fit for a multi-window PWA because:

- all windows can subscribe to the same channel name
- the service worker can publish one message
- every open window can react in the same way
- each window can decide how much data it needs to reload

That is usually cleaner than manually looping through `self.clients.matchAll()` and posting to each client one by one.

## Why the Worker Writes the Database

In this pattern, the service worker is the writer.

That matters because IndexedDB does not emit a built-in "database changed" event you can rely on across the app.

So the actual rule is:

- after the service worker finishes the IndexedDB write, it broadcasts that the data changed

Not:

- "IndexedDB changed somehow, therefore broadcast"

The writer is the one that knows when the write really completed.

## The Core APIs

### Page -> Service Worker

The page sends a message:

```js
worker.postMessage({ type: "add-row" });
```

This is direct communication to the service worker.

### Service Worker -> IndexedDB

The worker writes to IndexedDB using `idb`:

```js
const db = await idb.openDB("test-indexdb-sw", 1, {
  upgrade(database) {
    if (!database.objectStoreNames.contains("rows")) {
      database.createObjectStore("rows", { keyPath: "id" });
    }
  },
});

await db.add("rows", row);
```

### Service Worker -> All Windows

The worker broadcasts:

```js
channel.postMessage({ type: "row-added", id: row.id });
```

### Windows -> IndexedDB

Each page hears the message and reloads:

```js
channel.addEventListener("message", async (event) => {
  if (event.data?.type !== "row-added") return;
  await render();
});
```

## Why Re-query IndexedDB Instead of Sending the Full Row

The broadcast could contain the full row data, but the demo intentionally does not do that.

It only sends a simple message like:

```js
{ type: "row-added", id: row.id }
```

Then the page re-queries IndexedDB.

This is useful because:

- IndexedDB stays the source of truth
- all windows behave the same way
- a window that missed a previous message can still recover by re-reading the DB
- the broadcast stays small and simple

## Debugging Notes

While testing, a few things were important:

- `navigator.serviceWorker.controller` can be `null` on first load
- getting the active worker at click time is more reliable for demos
- seeing a service worker go from "running" to "stopped" in DevTools is normal
- service workers are event-driven and are stopped when idle

For debugging message delivery, the demo temporarily used an extra broadcast:

- `worker-received-add-row`

That helps separate:

- message did not reach the service worker
- message reached the service worker, but the DB write failed later

## How This Maps to the Real App

The same pattern can be used for orders:

1. The service worker receives order snapshot/delta data.
2. The service worker writes the order updates into IndexedDB.
3. The service worker broadcasts something like:

```js
{ type: "orders-updated" }
```

4. Each app window hears the broadcast.
5. Each window re-queries IndexedDB and refreshes its UI.

Possible refinements:

- broadcast only IDs that changed
- broadcast the current session/version
- let pages decide whether to patch in-memory state or fully re-read the DB

## Summary

The pattern to remember is:

- `postMessage` to tell the service worker to do work
- IndexedDB as the persistent shared store
- `BroadcastChannel` to tell every window that the shared data changed
- each window re-reads IndexedDB after hearing the broadcast

That is a good base model for a multi-window PWA.
