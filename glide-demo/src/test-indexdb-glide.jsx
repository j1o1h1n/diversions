import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import DataEditor, { GridCellKind } from "@glideapps/glide-data-grid";
import "@glideapps/glide-data-grid/dist/index.css";
import { openDB } from "idb";

const DB_NAME = "test-indexdb-glide";
const STORE_NAME = "orders";
const DB_VERSION = 1;
const CHANNEL_NAME = "test-indexdb-glide";

const COLUMNS = [
  { key: "orderid", title: "OrderId", width: 170, type: "number" },
  { key: "destination", title: "Destination", width: 110, type: "string" },
  { key: "client", title: "Client", width: 180, type: "string" },
  { key: "exchange", title: "Ex.", width: 90, type: "string" },
  { key: "symbol", title: "Symbol", width: 110, type: "string" },
  { key: "side", title: "Side", width: 110, type: "string" },
  { key: "shares", title: "Shares", width: 110, type: "number" },
  { key: "price", title: "Price", width: 110, type: "number" },
  { key: "value", title: "Value", width: 140, type: "number" },
  { key: "state", title: "State", width: 110, type: "string" },
  { key: "executed_shares", title: "Exec.", width: 120, type: "number" },
  { key: "cum_value", title: "Cum.", width: 130, type: "number" },
  { key: "perc_done", title: "Done", width: 100, type: "number" },
];

const moneyFormatter = new Intl.NumberFormat("en-US", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const integerFormatter = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 0,
});

const doneFormatter = new Intl.NumberFormat("en-US", {
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
});

function getDb() {
  return openDB(DB_NAME, DB_VERSION, {
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

async function loadOrders() {
  const db = await getDb();
  return db.getAll(STORE_NAME);
}

function formatValue(column, order) {
  const value = order[column.key];
  if (column.key === "price" || column.key === "value" || column.key === "cum_value") {
    return moneyFormatter.format(Number(value) || 0);
  }
  if (column.key === "shares" || column.key === "executed_shares") {
    return integerFormatter.format(Number(value) || 0);
  }
  if (column.key === "perc_done") {
    return doneFormatter.format(Number(value) || 0);
  }
  return String(value ?? "");
}

function getCell(column, order) {
  const display = formatValue(column, order);
  if (column.type === "number") {
    return {
      kind: GridCellKind.Number,
      data: Number(order[column.key]) || 0,
      displayData: display,
      allowOverlay: false,
      readonly: true,
      thousandSeparator: true,
      contentAlign: "right",
    };
  }
  return {
    kind: GridCellKind.Text,
    data: display,
    displayData: display,
    allowOverlay: false,
    readonly: true,
  };
}

function App() {
  const [orders, setOrders] = useState([]);
  const [status, setStatus] = useState("Loading...");

  const channel = useMemo(() => new BroadcastChannel(CHANNEL_NAME), []);
  const gridColumns = useMemo(
    () => COLUMNS.map((column) => ({ id: column.key, title: column.title, width: column.width })),
    []
  );

  useEffect(() => {
    let cancelled = false;

    async function reloadOrders(nextStatus) {
      const currentOrders = await loadOrders();
      if (!cancelled) {
        setOrders([...currentOrders].sort((a, b) => b.orderid - a.orderid));
        setStatus(nextStatus ?? `Loaded ${currentOrders.length.toLocaleString()} orders`);
      }
    }

    async function handleUpdateMessage(data) {
      if (!data || (data.type !== "orders-updated" && data.type !== "orders-cleared")) {
        return;
      }
      await reloadOrders(
        data.type === "orders-cleared"
          ? "Orders cleared"
          : `Reloaded after ${Number(data.count || 0).toLocaleString()} new orders`
      );
    }

    async function init() {
      await navigator.serviceWorker.register("./test-indexdb-glide-sw.js");
      await navigator.serviceWorker.ready;
      await reloadOrders("Ready");
    }

    async function handleMessage(event) {
      await handleUpdateMessage(event.data);
    }

    async function handleWorkerMessage(event) {
      await handleUpdateMessage(event.data);
    }

    channel.addEventListener("message", handleMessage);
    navigator.serviceWorker.addEventListener("message", handleWorkerMessage);
    init().catch((error) => {
      if (!cancelled) {
        setStatus(`Failed: ${error}`);
      }
    });

    return () => {
      cancelled = true;
      channel.removeEventListener("message", handleMessage);
      navigator.serviceWorker.removeEventListener("message", handleWorkerMessage);
      channel.close();
    };
  }, [channel]);

  async function addOrders() {
    const registration = await navigator.serviceWorker.ready;
    const worker = navigator.serviceWorker.controller || registration.active;
    worker?.postMessage({ type: "add-fake-orders", count: 10000 });
    setStatus("Sent request to service worker...");
    window.setTimeout(async () => {
      const currentOrders = await loadOrders();
      setOrders([...currentOrders].sort((a, b) => b.orderid - a.orderid));
      setStatus(`Loaded ${currentOrders.length.toLocaleString()} orders`);
    }, 400);
  }

  async function clearOrders() {
    const registration = await navigator.serviceWorker.ready;
    const worker = navigator.serviceWorker.controller || registration.active;
    worker?.postMessage({ type: "clear-orders" });
    setStatus("Sent clear request to service worker...");
    window.setTimeout(async () => {
      const currentOrders = await loadOrders();
      setOrders([...currentOrders].sort((a, b) => b.orderid - a.orderid));
      setStatus(`Loaded ${currentOrders.length.toLocaleString()} orders`);
    }, 250);
  }

  return (
    <main className="page">
      <section className="toolbar">
        <div>
          <h1>Glide + IndexedDB Demo</h1>
          <p>{status}</p>
        </div>
        <div className="actions">
          <button type="button" onClick={addOrders}>Add Orders</button>
          <button type="button" className="ghost" onClick={clearOrders}>Clear Orders</button>
        </div>
      </section>
      <section className="grid-wrap">
        <DataEditor
          width="100%"
          height="100%"
          columns={gridColumns}
          rows={orders.length}
          rowHeight={38}
          headerHeight={40}
          smoothScrollX={true}
          smoothScrollY={true}
          getCellContent={([col, row]) => {
            const order = orders[row];
            const column = COLUMNS[col];
            if (!order || !column) {
              return {
                kind: GridCellKind.Text,
                data: "",
                displayData: "",
                allowOverlay: false,
                readonly: true,
              };
            }
            return getCell(column, order);
          }}
        />
      </section>
    </main>
  );
}

const root = document.getElementById("root");
if (!root) {
  throw new Error("Missing root");
}

createRoot(root).render(<App />);
