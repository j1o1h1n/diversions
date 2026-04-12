# jscream

Minimal Maven Java library for single-threaded, reusable-buffer JSON encoding, token decoding, and parsed tape-style access.

The current implementation is intentionally narrow:

- Encoder state is reusable and buffer-backed.
- Decoder is pull-style and returns reusable views.
- Parser builds a reusable `JScreamTape` and returns lightweight `JSValue` wrappers over it.
- String tokens are carried as raw source slices and decoded lazily on access when escapes are present.
- Floating point formatting currently delegates to `Double.toString(...)`, which may allocate.
- The decoder currently supports ASCII JSON text plus standard string escapes. The encoder emits non-ASCII characters as `\uXXXX`.

## TODO

- Use [FastDoubleParser](https://github.com/wrandelshofer/FastDoubleParser/tree/main) for no-String double parsing

## Status

This is a starting point for a specialised low-allocation codec, not a full JSON library.

- Expected use is single threaded.
- Callers are expected to reuse `ByteArrayBuilder`, `JScreamEncoder`, `JScreamDecoder`, and `JScreamParser`.
- Decoder string values are returned through a reusable mutable view. Read or copy them before the next `next()` call if you need to retain content.
- Parsed `JSValue`, `JSArray`, `JSObject`, and `ByteSlice` instances are reusable views backed by `JScreamTape`.
- `JSObject.forEach(...)` reuses a single mutable `Entry` view per iteration. Copy out fields if you need to retain them.

## Build

```bash
cd jscream
mvn test
```

## Encode

```java
import com.helianthi.jscream.ByteArrayBuilder;
import com.helianthi.jscream.JScreamBuilder;
import com.helianthi.jscream.JScreamEncoder;

ByteArrayBuilder buff = new ByteArrayBuilder(1024);
JScreamEncoder encoder = new JScreamEncoder();

JScreamBuilder builder = encoder.prepare(buff).object();
builder.key("foo").value(1111);
builder.key("bar").array().value(2222).value("bandicoot").value(3.141592653589).done();
builder.key("baz").object().key("little").value("red").key("riding").value("hood").done();
builder.key("qux").value(false).done();

CharSequence out = buff.toCharSequence();
```

The output is:

```json
{"foo":1111,"bar":[2222,"bandicoot",3.141592653589],"baz":{"little":"red","riding":"hood"},"qux":false}
```

## Decode

```java
import com.helianthi.jscream.JScreamDecoder;
import com.helianthi.jscream.JSToken;

JScreamDecoder decoder = new JScreamDecoder().prepare(buff);

for (JSToken token = decoder.next(); token != JSToken.EOF; token = decoder.next()) {
    switch (token) {
        case FIELD_NAME:
        case STRING:
            System.out.println(decoder.stringValue());
            break;
        case LONG:
            System.out.println(decoder.longValue());
            break;
        case NUMBER:
            System.out.println(decoder.numberText());
            break;
        default:
            System.out.println(token);
            break;
    }
}
```

## Parse

```java
import com.helianthi.jscream.ByteArrayBuilder;
import com.helianthi.jscream.JSArray;
import com.helianthi.jscream.JSObject;
import com.helianthi.jscream.JSValue;
import com.helianthi.jscream.JScreamParser;

ByteArrayBuilder buff = new ByteArrayBuilder(128);
buff.appendAscii("{\"t\":354,\"d\":[{\"a\":1,\"b\":\"2\"},null,[99,88,77],{\"a\":100,\"b\":200},false],\"n\":null,\"s\":\"hello\",\"x\":9876}");

JScreamParser parser = new JScreamParser();
JSValue root = parser.parse(buff);

JSObject object = root.objectValue();
object.forEach(entry -> {
    System.out.println(entry.key + " -> " + entry.value.valueType());
});
```

Typical typed access looks like:

```java
JSValue root = parser.parse(buff);
JSObject object = root.objectValue();

final JSValue[] data = new JSValue[1];
object.forEach(entry -> {
    if ("d".contentEquals(entry.key)) {
        data[0] = entry.value;
    }
});

JSArray array = data[0].arrayValue();
array.forEach(item -> System.out.println(item.valueType()));
```

## Layout

```text
jscream/
  pom.xml
  src/main/java/com/helianthi/jscream/
  src/test/java/com/helianthi/jscream/
```
