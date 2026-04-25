package com.helianthi.jscream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class ByteSliceIntMapTest {

    @Test
    void storesKeyHandlesAndFindsValuesByObjectEntryAndKey() {
        JScreamTape tape = parseTape("{\"diamond\":\"clear\",\"ruby\":\"red\",\"emerald\":\"green\"}");
        ByteSliceIntMap map = new ByteSliceIntMap(tape);

        map.put(11, asciiKey("diamond"), 1);
        map.put(11, asciiKey("ruby"), 3);
        map.put(11, asciiKey("emerald"), 5);
        map.put(12, asciiKey("diamond"), 1);

        assertEquals(1, map.get(11, asciiKey("diamond")));
        assertEquals(3, map.get(11, asciiKey("ruby")));
        assertEquals(5, map.get(11, asciiKey("emerald")));
        assertEquals(-1, map.get(11, asciiKey("missing")));
        assertEquals(1, map.get(12, asciiKey("diamond")));

        ByteSlice value = new ByteSlice();
        assertEquals("clear", tape.value(map.get(11, asciiKey("diamond")) + 1, new JSValue()).stringValue(value).toString());
        assertEquals("red", tape.value(map.get(11, asciiKey("ruby")) + 1, new JSValue()).stringValue(value).toString());
        assertEquals("green", tape.value(map.get(11, asciiKey("emerald")) + 1, new JSValue()).stringValue(value).toString());
    }

    @Test
    void clearsAllEntries() {
        JScreamTape tape = parseTape("{\"diamond\":\"clear\"}");
        ByteSliceIntMap map = new ByteSliceIntMap(tape);

        map.put(7, asciiKey("diamond"), 1);
        assertEquals(1, map.get(7, asciiKey("diamond")));

        map.clear();

        assertEquals(-1, map.get(7, asciiKey("diamond")));
    }

    private JScreamTape parseTape(String json) {
        ByteArrayBuilder buffer = new ByteArrayBuilder(json.length());
        buffer.append(json);

        JScreamDecoder decoder = new JScreamDecoder().prepare(buffer);
        JScreamTape tape = new JScreamTape();
        tape.prepare(buffer.buffer());
        for (JSToken token = decoder.next(); token != JSToken.EOF; token = decoder.next()) {
            switch (token) {
                case FIELD_NAME:
                case STRING:
                    tape.addString(decoder.stringValue());
                    break;
                case START_OBJECT:
                    tape.addObject(decoder.tokenStart());
                    break;
                case END_OBJECT:
                    tape.endObject(decoder.tokenEnd());
                    break;
                default:
                    throw new IllegalStateException("unexpected token " + token);
            }
        }
        tape.close();
        return tape;
    }

    private ByteSlice asciiKey(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
        ByteSlice key = new ByteSlice();
        key.use(bytes, 0, bytes.length);
        return key;
    }
}
