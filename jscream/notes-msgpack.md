```json
{"updates": [
  ["symbol", "bid", "ask", "last"],
  ["BHP", 45.12, 45.13, 45.12],
  ["RIO", 119.8, 119.9, 119.85]
]}
```

Pack on the server:

```java

MessagePacker packer;

packer.packMapHeader(1);
packer.packRawStringHeader(7).addPayload("updates")

packer.packArrayHeader(3);

packer.packArrayHeader(4);
packer.packRawStringHeader(6).addPayload("symbol")
packer.packRawStringHeader(3).addPayload("bid")
packer.packRawStringHeader(3).addPayload("ask")
packer.packRawStringHeader(4).addPayload("last")

packer.packRawStringHeader(3).addPayload("BHP")
packer.addDouble(45.12)
packer.addDouble(45.13)
packer.addDouble(45.12)

packer.packRawStringHeader(3).addPayload("RIO")
packer.addDouble(119.8)
packer.addDouble(119.9)
packer.addDouble(119.85)
```

Handle on the client:


```JavaScript
import { decode } from "@msgpack/msgpack";

const ws = new WebSocket("ws://localhost:8080");
ws.binaryType = "arraybuffer";

ws.onmessage = (event) => {
  const [fields, ...rows] = decode(new Uint8Array(event.data));
  processRows(fields, rows);
};

processRows(felds, rows) {
const obj = {};
for (const row of rows) {
  for (let i = 0; i < fields.length; i++) {
    obj[fields[i]] = row[i];
  }
  handle(obj);
}
}
```
