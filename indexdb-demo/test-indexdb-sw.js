importScripts("https://cdn.jsdelivr.net/npm/idb@8/build/umd.js");

const channel = new BroadcastChannel("test-indexdb");

self.addEventListener("install", function () {
  self.skipWaiting();
});

self.addEventListener("activate", function (event) {
  event.waitUntil(self.clients.claim());
});

self.addEventListener("message", function (event) {
  if (!event.data || event.data.type !== "add-row") {
    return;
  }

  event.waitUntil((async function () {
    channel.postMessage({ type: "worker-received-add-row" });

    // Open the same IndexedDB database the page reads from.
    const db = await idb.openDB("test-indexdb-sw", 1, {
      upgrade(database) {
        if (!database.objectStoreNames.contains("rows")) {
          database.createObjectStore("rows", { keyPath: "id" });
        }
      },
    });

    const row = {
      id: Date.now(),
      name: "Row " + Math.floor(Math.random() * 1000),
      createdAt: new Date().toLocaleTimeString(),
    };

    // The service worker is the writer in this demo.
    await db.add("rows", row);

    // After the write completes, broadcast that the data changed.
    channel.postMessage({ type: "row-added", id: row.id });
  })());
});
