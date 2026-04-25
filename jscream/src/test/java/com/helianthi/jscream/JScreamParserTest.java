package com.helianthi.jscream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.randelshofer.fastdoubleparser.JavaDoubleParser;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.junit.jupiter.api.Test;

class JScreamParserTest {
    @Test
    void parsesSimpleLong() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(16);
        buffer.append("1234");

        JSValue value = new JScreamParser().parse(buffer, new JSValue());

        assertEquals(JScreamTape.INT64, value.valueType());
        assertEquals(1234L, value.longValue());
    }

    @Test
    void parsesSimpleDouble() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(16);
        buffer.append("3.14");

        JSValue value = new JScreamParser().parse(buffer, new JSValue());

        assertEquals(JScreamTape.DOUBLE, value.valueType());
        assertEquals(3.14d, value.doubleValue());
    }

    @Test
    void usesConfiguredDoubleParser() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(16);
        buffer.append("3.14");

        JSValue value = new JScreamParser((bytes, offset, length) -> 42.5d).parse(buffer, new JSValue());

        assertEquals(JScreamTape.DOUBLE, value.valueType());
        assertEquals(42.5d, value.doubleValue());
    }

    @Test
    void usesFastDoubleParser() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(32);
        buffer.append("12345.6789");

        JSValue value = new JScreamParser(JavaDoubleParser::parseDouble).parse(buffer, new JSValue());

        assertEquals(JScreamTape.DOUBLE, value.valueType());
        assertEquals(12345.6789d, value.doubleValue());
    }

    @Test
    void parsesSimpleString() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(16);
        buffer.append("\"hello\"");

        JSValue value = new JScreamParser().parse(buffer, new JSValue());
        ByteSlice string = new ByteSlice();

        assertEquals(JScreamTape.STRING, value.valueType());
        assertEquals("hello", value.stringValue(string).toString());
    }

    @Test
    void parsesUtf8String() {
        ByteArrayBuilder buffer = utf8Buffer("{\"city\":\"São Paulo\"}");

        JSValue root = new JScreamParser().parse(buffer, new JSValue());
        JSObject object = root.objectValue(new JSObject());
        ByteSlice string = new ByteSlice();

        assertEquals("São Paulo", object.valueAt(0, new JSValue()).stringValue(string).toString());
    }

    @Test
    void decodesUnicodeEscapesToUtf8() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(64);
        buffer.append("{\"text\":\"caf\\u00e9 \\uD83D\\uDE03\"}");

        JSValue root = new JScreamParser().parse(buffer, new JSValue());
        JSObject object = root.objectValue(new JSObject());
        ByteSlice string = new ByteSlice();

        assertEquals("café 😃", object.valueAt(0, new JSValue()).stringValue(string).toString());
    }

    @Test
    void parsesTrueBoolean() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(16);
        buffer.append("true");

        JSValue value = new JScreamParser().parse(buffer, new JSValue());

        assertEquals(JScreamTape.BOOL, value.valueType());
        assertTrue(value.boolValue());
    }

    @Test
    void parsesFalseBoolean() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(16);
        buffer.append("false");

        JSValue value = new JScreamParser().parse(buffer, new JSValue());

        assertEquals(JScreamTape.BOOL, value.valueType());
        assertFalse(value.boolValue());
    }

    @Test
    void parsesNull() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(16);
        buffer.append("null");

        JSValue value = new JScreamParser().parse(buffer, new JSValue());

        assertEquals(JScreamTape.NULL_VALUE, value.valueType());
    }

    @Test
    void parsesArray() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(32);
        buffer.append("[1,\"two\",false]");

        JSValue value = new JScreamParser().parse(buffer, new JSValue());
        JSArray array = value.arrayValue(new JSArray());
        JSValue first = array.valueAt(0, new JSValue());
        JSValue second = array.valueAt(1, new JSValue());
        JSValue third = array.valueAt(2, new JSValue());
        ByteSlice string = new ByteSlice();

        assertEquals(JScreamTape.ARRAY, value.valueType());
        assertEquals(3, array.size());
        assertEquals(JScreamTape.INT64, first.valueType());
        assertEquals(1L, first.longValue());
        assertEquals(JScreamTape.STRING, second.valueType());
        assertEquals("two", second.stringValue(string).toString());
        assertEquals(JScreamTape.BOOL, third.valueType());
        assertFalse(third.boolValue());
    }

    @Test
    void parsesObject() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(32);
        buffer.append("{\"a\":1}");

        JSValue value = new JScreamParser().parse(buffer, new JSValue());
        JSObject object = value.objectValue(new JSObject());
        ByteSlice key = object.keyAt(0, new ByteSlice());
        JSValue parsedValue = object.valueAt(0, new JSValue());

        assertEquals(JScreamTape.OBJECT, value.valueType());
        assertEquals(1, object.size());
        assertEquals("a", key.toString());
        assertEquals(JScreamTape.INT64, parsedValue.valueType());
        assertEquals(1L, parsedValue.longValue());
    }

    @Test
    void looksUpObjectValuesByKey() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(64);
        buffer.append("{\"a\":1,\"b\":true}");

        JSValue root = new JScreamParser().parse(buffer, new JSValue());
        JSObject object = root.objectValue(new JSObject());
        ByteSlice a = new ByteSlice();
        a.use("a".getBytes(), 0, 1);
        ByteSlice b = new ByteSlice();
        b.use("b".getBytes(), 0, 1);
        ByteSlice missing = new ByteSlice();
        missing.use("z".getBytes(), 0, 1);

        assertTrue(object.containsKey(a));
        assertTrue(object.containsKey(b));
        assertFalse(object.containsKey(missing));
        assertEquals(1L, object.get(a, new JSValue()).longValue());
        assertTrue(object.get(b, new JSValue()).boolValue());
        assertNull(object.get(missing, new JSValue()));
    }

    @Test
    void parsesReferenceJsonFixturesFromResources() {
        assertEquals(JScreamTape.OBJECT, parseResource("json/rfc-example-object.json").valueType());
        assertEquals(JScreamTape.ARRAY, parseResource("json/rfc-example-array.json").valueType());
        assertEquals(JScreamTape.OBJECT, parseResource("json/jsonsuite-valid-basic-object.json").valueType());
        assertEquals(JScreamTape.OBJECT, parseResource("json/canada.json").valueType());
        assertEquals(JScreamTape.OBJECT, parseResource("json/citm_catalog.json").valueType());
        assertEquals(JScreamTape.OBJECT, parseResource("json/gemstones.json").valueType());
    }

    @Test
    void hashesValuesFromHardCodedRfcObjectLookups() {
        JSValue root = parseResource("json/rfc-example-object.json");
        JSObject rootObject = root.objectValue(new JSObject());
        JSObject image = rootObject.get(asciiKey("Image"), new JSValue()).objectValue(new JSObject());
        JSObject thumbnail = image.get(asciiKey("Thumbnail"), new JSValue()).objectValue(new JSObject());
        JSArray ids = image.get(asciiKey("IDs"), new JSValue()).arrayValue(new JSArray());
        ByteSlice string = new ByteSlice();

        int hash = 1;
        hash = (31 * hash) + Long.hashCode(image.get(asciiKey("Width"), new JSValue()).longValue());
        hash = (31 * hash) + Long.hashCode(image.get(asciiKey("Height"), new JSValue()).longValue());
        hash = (31 * hash) + image.get(asciiKey("Title"), new JSValue()).stringValue(string).toString().hashCode();
        hash = (31 * hash) + thumbnail.get(asciiKey("Url"), new JSValue()).stringValue(string).toString().hashCode();
        hash = (31 * hash) + Long.hashCode(thumbnail.get(asciiKey("Height"), new JSValue()).longValue());
        hash = (31 * hash) + Long.hashCode(thumbnail.get(asciiKey("Width"), new JSValue()).longValue());
        hash = (31 * hash) + Boolean.hashCode(image.get(asciiKey("Animated"), new JSValue()).boolValue());
        hash = (31 * hash) + Long.hashCode(ids.valueAt(0, new JSValue()).longValue());
        hash = (31 * hash) + Long.hashCode(ids.valueAt(1, new JSValue()).longValue());
        hash = (31 * hash) + Long.hashCode(ids.valueAt(2, new JSValue()).longValue());
        hash = (31 * hash) + Long.hashCode(ids.valueAt(3, new JSValue()).longValue());

        assertEquals(-19782094, hash);
    }

    @Test
    void sumsBerlinerPhilharmonikerPerformancePricesFromCitmCatalog() {
        JSValue root = parseResource("json/citm_catalog.json");
        JSObject catalog = root.objectValue(new JSObject());
        JSObject events = catalog.get(asciiKey("events"), new JSValue()).objectValue(new JSObject());
        JSArray performances = catalog.get(asciiKey("performances"), new JSValue()).arrayValue(new JSArray());
        ByteSlice string = new ByteSlice();

        long total = 0L;
        for (int i = 0; i < performances.size(); i++) {
            JSObject performance = performances.valueAt(i, new JSValue()).objectValue(new JSObject());
            long eventId = performance.get(asciiKey("eventId"), new JSValue()).longValue();
            JSValue eventValue = events.get(asciiKey(Long.toString(eventId)), new JSValue());
            if (eventValue == null) {
                continue;
            }

            JSObject event = eventValue.objectValue(new JSObject());
            String eventName = event.get(asciiKey("name"), new JSValue()).stringValue(string).toString();
            if (!"Berliner Philharmoniker".equals(eventName)) {
                continue;
            }

            JSArray prices = performance.get(asciiKey("prices"), new JSValue()).arrayValue(new JSArray());
            for (int j = 0; j < prices.size(); j++) {
                JSObject price = prices.valueAt(j, new JSValue()).objectValue(new JSObject());
                total += price.get(asciiKey("amount"), new JSValue()).longValue();
            }
        }

        assertEquals(789500L, total);
    }

    @Test
    void countsECharactersAcrossDeterministicRandomGemstoneLookups() {
        JSValue root = parseResource("json/gemstones.json");
        JSObject gemstones = root.objectValue(new JSObject());
        ByteSlice string = new ByteSlice();
        String[] knownKeys = {
            "diamond", "ruby", "sapphire", "emerald", "amethyst",
            "topaz", "opal", "garnet", "aquamarine", "peridot",
            "turquoise", "citrine", "spinel", "zircon", "tourmaline",
            "tanzanite", "moonstone", "sunstone", "lapis", "jade",
            "onyx", "agate", "jasper", "carnelian", "chalcedony",
            "alexandrite", "kunzite", "morganite", "heliodor", "iolite",
            "kyanite", "andalusite", "apatite", "diopside", "tsavorite",
            "rhodolite", "spessartine", "pyrope", "serpentine", "malachite"
        };
        int[] expectedECounts = {
            5, 2, 3, 5, 3,
            5, 2, 6, 7, 7,
            6, 2, 3, 2, 5,
            3, 4, 4, 4, 7,
            3, 5, 2, 4, 2,
            5, 5, 5, 3, 5,
            4, 5, 6, 6, 4,
            6, 2, 3, 8, 6
        };
        Random random = new Random();

        int expected = 0;
        int count = 0;
        for (int i = 0; i < 10; i++) {
            int index = random.nextInt(knownKeys.length);
            String key = knownKeys[index];
            expected += expectedECounts[index];
            String value = gemstones.get(asciiKey(key), new JSValue()).stringValue(string).toString();
            count += countLowercaseE(value);
        }

        assertEquals(expected, count);
    }

    @Test
    void rejectsInvalidReferenceJsonFixture() {
        assertThrows(IllegalArgumentException.class,
            () -> parseResource("json/jsonsuite-invalid-trailing-comma.json"));
    }

    @Test
    void getsRawContainerJson() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(64);
        buffer.append("{\"a\":[1,{\"b\":2}],\"c\":true}");

        JSValue root = new JScreamParser().parse(buffer, new JSValue());
        JSObject object = root.objectValue(new JSObject());
        JSValue arrayValue = object.valueAt(0, new JSValue());
        JSArray array = arrayValue.arrayValue(new JSArray());
        JSValue nestedObjectValue = array.valueAt(1, new JSValue());
        JSObject nestedObject = nestedObjectValue.objectValue(new JSObject());
        ByteSlice raw = new ByteSlice();

        assertEquals("{\"a\":[1,{\"b\":2}],\"c\":true}", object.rawValue(raw).toString());
        assertEquals("[1,{\"b\":2}]", array.rawValue(raw).toString());
        assertEquals("{\"b\":2}", nestedObject.rawValue(raw).toString());
    }

    @Test
    void parsesNestedStructure() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(64);
        buffer.append("{\"a\":[1],\"b\":{\"c\":true}}");

        JSValue root = new JScreamParser().parse(buffer, new JSValue());
        JSObject object = root.objectValue(new JSObject());
        ByteSlice key = new ByteSlice();
        JSValue firstValue = object.valueAt(0, new JSValue());
        JSValue secondValue = object.valueAt(1, new JSValue());

        assertEquals(JScreamTape.OBJECT, root.valueType());
        assertEquals(2, object.size());
        assertEquals("a", object.keyAt(0, key).toString());
        assertEquals(JScreamTape.ARRAY, firstValue.valueType());
        JSArray array = firstValue.arrayValue(new JSArray());
        JSValue arrayItem = array.valueAt(0, new JSValue());
        assertEquals(1L, arrayItem.longValue());

        assertEquals("b", object.keyAt(1, key).toString());
        assertEquals(JScreamTape.OBJECT, secondValue.valueType());
        JSObject nestedObject = secondValue.objectValue(new JSObject());
        JSValue nestedValue = nestedObject.valueAt(0, new JSValue());
        assertEquals("c", nestedObject.keyAt(0, key).toString());
        assertTrue(nestedValue.boolValue());
    }

    @Test
    void parsesMixedDeepStructure() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(128);
        buffer.append("{\"t\":354,\"d\":[{\"a\":1,\"b\":\"2\"},null,[99,88,77],{\"a\":100,\"b\":200},false],\"n\":null,\"s\":\"hello\",\"x\":9876}");

        JSValue root = new JScreamParser().parse(buffer, new JSValue());
        JSObject object = root.objectValue(new JSObject());
        ByteSlice key = new ByteSlice();
        ByteSlice string = new ByteSlice();
        JSValue t = object.valueAt(0, new JSValue());
        JSValue dValue = object.valueAt(1, new JSValue());
        JSValue n = object.valueAt(2, new JSValue());
        JSValue s = object.valueAt(3, new JSValue());
        JSValue x = object.valueAt(4, new JSValue());

        assertEquals(JScreamTape.OBJECT, root.valueType());
        assertEquals(5, object.size());

        assertEquals("t", object.keyAt(0, key).toString());
        assertEquals(354L, t.longValue());

        assertEquals("d", object.keyAt(1, key).toString());
        JSArray d = dValue.arrayValue(new JSArray());
        assertEquals(5, d.size());
        JSValue firstItem = d.valueAt(0, new JSValue());
        JSValue nullItem = d.valueAt(1, new JSValue());
        JSValue nestedArrayItem = d.valueAt(2, new JSValue());
        JSValue secondObjectItem = d.valueAt(3, new JSValue());
        JSValue falseItem = d.valueAt(4, new JSValue());

        JSObject firstObject = firstItem.objectValue(new JSObject());
        JSValue firstA = firstObject.valueAt(0, new JSValue());
        JSValue firstB = firstObject.valueAt(1, new JSValue());
        assertEquals("a", firstObject.keyAt(0, key).toString());
        assertEquals(1L, firstA.longValue());
        assertEquals("b", firstObject.keyAt(1, key).toString());
        assertEquals("2", firstB.stringValue(string).toString());

        assertEquals(JScreamTape.NULL_VALUE, nullItem.valueType());

        JSArray nestedArray = nestedArrayItem.arrayValue(new JSArray());
        assertEquals(99L, nestedArray.valueAt(0, new JSValue()).longValue());
        assertEquals(88L, nestedArray.valueAt(1, new JSValue()).longValue());
        assertEquals(77L, nestedArray.valueAt(2, new JSValue()).longValue());

        JSObject secondObject = secondObjectItem.objectValue(new JSObject());
        JSValue secondA = secondObject.valueAt(0, new JSValue());
        JSValue secondB = secondObject.valueAt(1, new JSValue());
        assertEquals("a", secondObject.keyAt(0, key).toString());
        assertEquals(100L, secondA.longValue());
        assertEquals("b", secondObject.keyAt(1, key).toString());
        assertEquals(200L, secondB.longValue());

        assertFalse(falseItem.boolValue());

        assertEquals("n", object.keyAt(2, key).toString());
        assertEquals(JScreamTape.NULL_VALUE, n.valueType());

        assertEquals("s", object.keyAt(3, key).toString());
        assertEquals("hello", s.stringValue(string).toString());

        assertEquals("x", object.keyAt(4, key).toString());
        assertEquals(9876L, x.longValue());
    }

    private JSValue parseResource(String resourceName) {
        ByteArrayBuilder buffer = readResource(resourceName);
        return new JScreamParser(JavaDoubleParser::parseDouble).parse(buffer, new JSValue());
    }

    private ByteArrayBuilder readResource(String resourceName) {
        try (InputStream in = JScreamParserTest.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (in == null) {
                throw new IllegalArgumentException("missing resource " + resourceName);
            }
            byte[] bytes = in.readAllBytes();
            ByteArrayBuilder buffer = new ByteArrayBuilder(bytes.length);
            for (byte b : bytes) {
                buffer.appendByte(b);
            }
            return buffer;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private ByteArrayBuilder utf8Buffer(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        ByteArrayBuilder buffer = new ByteArrayBuilder(bytes.length);
        for (byte b : bytes) {
            buffer.appendByte(b);
        }
        return buffer;
    }

    private ByteSlice asciiKey(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
        ByteSlice key = new ByteSlice();
        key.use(bytes, 0, bytes.length);
        return key;
    }

    private int countLowercaseE(String value) {
        int count = 0;
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) == 'e') {
                count++;
            }
        }
        return count;
    }
}
