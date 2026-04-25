package com.helianthi.jscream;

import java.nio.charset.StandardCharsets;

/**
 * Reusable mutable byte slice used by the decoder. The content is overwritten
 * by the next string token.
 */
public final class ByteSlice {
    private byte[] bytes = new byte[0];
    private int pos;
    private int length;

    public ByteSlice() {
    }

    void use(byte[] bytes, int pos, int length) {
        this.bytes = bytes;
        this.pos = pos;
        this.length = length;
    }

    public int pos() {
        return pos;
    }

    public byte[] bytes() {
        return bytes;
    }

    public int length() {
        return length;
    }

    @Override
    public String toString() {
        return new String(bytes, pos, length, StandardCharsets.UTF_8);
    }
}
