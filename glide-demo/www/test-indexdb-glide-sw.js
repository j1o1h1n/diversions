importScripts("https://cdn.jsdelivr.net/npm/idb@8/build/umd.js");

const DB_NAME = "test-indexdb-glide";
const STORE_NAME = "orders";
const DB_VERSION = 1;
const CHANNEL_NAME = "test-indexdb-glide";
const channel = new BroadcastChannel(CHANNEL_NAME);

const clientsList = [
  "AlphaFund",
  "BlueRiver",
  "Capital Markets",
  "Daintree AM",
  "Ellcore Bank",
  "Figaro Heavy Industries",
  "Gan Gan & Co.",
  "Harbor Street",
  "Ironbark",
  "Kestrel",
  "Acacia Capital",
  "North Ridge",
  "Quartz Street",
  "Silver Fern",
];

const exchanges = ["AX", "NZ", "HK", "CN", "JP"];
const symbols = ["BHP", "CBA", "WBC", "ANZ", "NAB", "TLS", "MQG", "CSL"];
const sides = ["BUY", "SELL", "SHORT SELL"];
const states = ["New", "Part", "Filled", "Canceled"];

let nextOrderId = Date.now() * 1000;

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function pick(values) {
  return values[randomInt(0, values.length - 1)];
}

function makeOrder(client) {
  const shares = randomInt(1, 100) * 100;
  const price = Number((Math.random() * 250 + 1).toFixed(2));
  const done = Number((Math.random() * 100).toFixed(1));
  const executed = Math.round((shares * done) / 100);
  const cum = Number((executed * price).toFixed(2));

  nextOrderId += 1;

  return {
    orderid: nextOrderId,
    destination: "WORK",
    client,
    exchange: pick(exchanges),
    symbol: pick(symbols),
    side: pick(sides),
    shares,
    price,
    value: Number((shares * price).toFixed(2)),
    state: pick(states),
    executed_shares: executed,
    cum_value: cum,
    perc_done: done,
  };
}

async function getDb() {
  return idb.openDB(DB_NAME, DB_VERSION, {
    upgrade(db) {
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        const store = db.createObjectStore(STORE_NAME, { keyPath: "orderid" });
        store.createIndex("orderid", "orderid", { unique: true });
        store.createIndex("client", "client", { unique: false });
        store.createIndex("exchange", "exchange", { unique: false });
      }
    },
  });
}

async function notifyAll(message) {
  channel.postMessage(message);
  const clients = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
  clients.forEach((client) => client.postMessage(message));
}

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

self.addEventListener("message", (event) => {
  if (!event.data) {
    return;
  }

  event.waitUntil((async () => {
    const db = await getDb();
    if (event.data.type === "clear-orders") {
      const tx = db.transaction(STORE_NAME, "readwrite");
      await tx.store.clear();
      await tx.done;
      await notifyAll({ type: "orders-cleared" });
      return;
    }

    if (event.data.type === "add-fake-orders") {
      const count = Number(event.data.count) || 10000;
      const tx = db.transaction(STORE_NAME, "readwrite");
      for (let i = 0; i < count; i += 1) {
        tx.store.put(makeOrder(pick(clientsList)));
      }
      await tx.done;
      await notifyAll({ type: "orders-updated", count });
    }
  })());
});
