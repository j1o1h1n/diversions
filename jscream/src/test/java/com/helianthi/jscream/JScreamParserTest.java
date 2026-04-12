package com.helianthi.jscream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class JScreamParserTest {
    @Test
    void parsesSimpleLong() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(16);
        buffer.appendAscii("1234");

        JSValue value = new JScreamParser().parse(buffer);

        assertEquals(JScreamTape.INT64, value.valueType());
        assertEquals(1234L, value.longValue());
    }

    @Test
    void parsesSimpleDouble() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(16);
        buffer.appendAscii("3.14");

        JSValue value = new JScreamParser().parse(buffer);

        assertEquals(JScreamTape.DOUBLE, value.valueType());
        assertEquals(3.14d, value.doubleValue());
    }

    @Test
    void parsesSimpleString() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(16);
        buffer.appendAscii("\"hello\"");

        JSValue value = new JScreamParser().parse(buffer);

        assertEquals(JScreamTape.STRING, value.valueType());
        assertEquals("hello", value.stringValue().toString());
    }

    @Test
    void parsesTrueBoolean() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(16);
        buffer.appendAscii("true");

        JSValue value = new JScreamParser().parse(buffer);

        assertEquals(JScreamTape.BOOL, value.valueType());
        assertTrue(value.boolValue());
    }

    @Test
    void parsesFalseBoolean() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(16);
        buffer.appendAscii("false");

        JSValue value = new JScreamParser().parse(buffer);

        assertEquals(JScreamTape.BOOL, value.valueType());
        assertFalse(value.boolValue());
    }

    @Test
    void parsesNull() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(16);
        buffer.appendAscii("null");

        JSValue value = new JScreamParser().parse(buffer);

        assertEquals(JScreamTape.NULL_VALUE, value.valueType());
    }

    @Test
    void parsesArray() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(32);
        buffer.appendAscii("[1,\"two\",false]");

        JSValue value = new JScreamParser().parse(buffer);
        JSArray array = value.arrayValue();
        JSValue[] items = new JSValue[3];
        int[] index = {0};

        array.forEach(item -> items[index[0]++] = item);

        assertEquals(JScreamTape.ARRAY, value.valueType());
        assertEquals(3, array.size());
        assertEquals(JScreamTape.INT64, items[0].valueType());
        assertEquals(1L, items[0].longValue());
        assertEquals(JScreamTape.STRING, items[1].valueType());
        assertEquals("two", items[1].stringValue().toString());
        assertEquals(JScreamTape.BOOL, items[2].valueType());
        assertFalse(items[2].boolValue());
    }

    @Test
    void parsesObject() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(32);
        buffer.appendAscii("{\"a\":1}");

        JSValue value = new JScreamParser().parse(buffer);
        JSObject object = value.objectValue();
        String[] key = new String[1];
        JSValue[] parsedValue = new JSValue[1];

        object.forEach(entry -> {
            key[0] = entry.key.toString();
            parsedValue[0] = entry.value;
        });

        assertEquals(JScreamTape.OBJECT, value.valueType());
        assertEquals(1, object.size());
        assertEquals("a", key[0]);
        assertEquals(JScreamTape.INT64, parsedValue[0].valueType());
        assertEquals(1L, parsedValue[0].longValue());
    }

    @Test
    void parsesNestedStructure() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(64);
        buffer.appendAscii("{\"a\":[1],\"b\":{\"c\":true}}");

        JSValue root = new JScreamParser().parse(buffer);
        JSObject object = root.objectValue();
        String[] keys = new String[2];
        JSValue[] values = new JSValue[2];
        int[] index = {0};

        object.forEach(entry -> {
            int i = index[0]++;
            keys[i] = entry.key.toString();
            values[i] = entry.value;
        });

        assertEquals(JScreamTape.OBJECT, root.valueType());
        assertEquals(2, object.size());
        assertEquals("a", keys[0]);
        assertEquals(JScreamTape.ARRAY, values[0].valueType());
        JSValue[] arrayItems = new JSValue[1];
        values[0].arrayValue().forEach(item -> arrayItems[0] = item);
        assertEquals(1L, arrayItems[0].longValue());

        assertEquals("b", keys[1]);
        assertEquals(JScreamTape.OBJECT, values[1].valueType());
        String[] nestedKey = new String[1];
        JSValue[] nestedValue = new JSValue[1];
        values[1].objectValue().forEach(entry -> {
            nestedKey[0] = entry.key.toString();
            nestedValue[0] = entry.value;
        });
        assertEquals("c", nestedKey[0]);
        assertTrue(nestedValue[0].boolValue());
    }

    @Test
    void parsesMixedDeepStructure() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(128);
        buffer.appendAscii("{\"t\":354,\"d\":[{\"a\":1,\"b\":\"2\"},null,[99,88,77],{\"a\":100,\"b\":200},false],\"n\":null,\"s\":\"hello\",\"x\":9876}");

        JSValue root = new JScreamParser().parse(buffer);
        JSObject object = root.objectValue();

        String[] keys = new String[5];
        JSValue[] values = new JSValue[5];
        int[] index = {0};
        object.forEach(entry -> {
            int i = index[0]++;
            keys[i] = entry.key.toString();
            values[i] = entry.value;
        });

        assertEquals(JScreamTape.OBJECT, root.valueType());
        assertEquals(5, object.size());

        assertEquals("t", keys[0]);
        assertEquals(354L, values[0].longValue());

        assertEquals("d", keys[1]);
        JSArray d = values[1].arrayValue();
        assertEquals(5, d.size());
        JSValue[] dItems = new JSValue[5];
        int[] dIndex = {0};
        d.forEach(item -> dItems[dIndex[0]++] = item);

        JSObject firstObject = dItems[0].objectValue();
        String[] firstKeys = new String[2];
        JSValue[] firstValues = new JSValue[2];
        int[] firstIndex = {0};
        firstObject.forEach(entry -> {
            int i = firstIndex[0]++;
            firstKeys[i] = entry.key.toString();
            firstValues[i] = entry.value;
        });
        assertEquals("a", firstKeys[0]);
        assertEquals(1L, firstValues[0].longValue());
        assertEquals("b", firstKeys[1]);
        assertEquals("2", firstValues[1].stringValue().toString());

        assertEquals(JScreamTape.NULL_VALUE, dItems[1].valueType());

        JSArray nestedArray = dItems[2].arrayValue();
        JSValue[] nestedArrayItems = new JSValue[3];
        int[] nestedArrayIndex = {0};
        nestedArray.forEach(item -> nestedArrayItems[nestedArrayIndex[0]++] = item);
        assertEquals(99L, nestedArrayItems[0].longValue());
        assertEquals(88L, nestedArrayItems[1].longValue());
        assertEquals(77L, nestedArrayItems[2].longValue());

        JSObject secondObject = dItems[3].objectValue();
        String[] secondKeys = new String[2];
        JSValue[] secondValues = new JSValue[2];
        int[] secondIndex = {0};
        secondObject.forEach(entry -> {
            int i = secondIndex[0]++;
            secondKeys[i] = entry.key.toString();
            secondValues[i] = entry.value;
        });
        assertEquals("a", secondKeys[0]);
        assertEquals(100L, secondValues[0].longValue());
        assertEquals("b", secondKeys[1]);
        assertEquals(200L, secondValues[1].longValue());

        assertFalse(dItems[4].boolValue());

        assertEquals("n", keys[2]);
        assertEquals(JScreamTape.NULL_VALUE, values[2].valueType());

        assertEquals("s", keys[3]);
        assertEquals("hello", values[3].stringValue().toString());

        assertEquals("x", keys[4]);
        assertEquals(9876L, values[4].longValue());
    }
}
