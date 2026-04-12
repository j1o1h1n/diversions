package com.helianthi.jscream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class JScreamCodecTest {
    @Test
    void encodesNestedJson() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(128);
        JScreamEncoder encoder = new JScreamEncoder();

        encoder.prepare(buffer)
            .object()
            .key("foo").value(1111)
            .key("bar").array().value(2222).value("bandicoot").value(3.141592653589).done()
            .key("baz").object().key("little").value("red").key("riding").value("hood").done()
            .key("qux").value(false)
            .done();

        assertEquals(
            "{\"foo\":1111,\"bar\":[2222,\"bandicoot\",3.141592653589],\"baz\":{\"little\":\"red\",\"riding\":\"hood\"},\"qux\":false}",
            buffer.toString()
        );
    }

    @Test
    void decodesTokenStream() {
        ByteArrayBuilder buffer = new ByteArrayBuilder(128);
        JScreamEncoder encoder = new JScreamEncoder();

        encoder.prepare(buffer)
            .object()
            .key("foo").value(1111)
            .key("bar").array().value(2222).value("bandicoot").done()
            .key("qux").value(false)
            .done();

        JScreamDecoder decoder = new JScreamDecoder().prepare(buffer);

        assertEquals(JSToken.START_OBJECT, decoder.next());
        assertEquals(JSToken.FIELD_NAME, decoder.next());
        assertEquals("foo", decoder.stringValue().toString());
        assertEquals(JSToken.LONG, decoder.next());
        assertEquals(1111L, decoder.longValue());
        assertEquals(JSToken.FIELD_NAME, decoder.next());
        assertEquals("bar", decoder.stringValue().toString());
        assertEquals(JSToken.START_ARRAY, decoder.next());
        assertEquals(JSToken.LONG, decoder.next());
        assertEquals(2222L, decoder.longValue());
        assertEquals(JSToken.STRING, decoder.next());
        assertEquals("bandicoot", decoder.stringValue().toString());
        assertEquals(JSToken.END_ARRAY, decoder.next());
        assertEquals(JSToken.FIELD_NAME, decoder.next());
        assertEquals("qux", decoder.stringValue().toString());
        assertEquals(JSToken.FALSE, decoder.next());
        assertEquals(JSToken.END_OBJECT, decoder.next());
        assertEquals(JSToken.EOF, decoder.next());
    }

}
