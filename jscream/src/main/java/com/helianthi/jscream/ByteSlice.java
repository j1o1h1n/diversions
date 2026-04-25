package com.helianthi.jscream;

import java.nio.charset.StandardCharsets;

/**
 * Reusable mutable byte slice used by the decoder. The content is overwritten
 * by the next string token.
 */
public final class ByteSlice {
    private static final int NOT_CALCULATED = Integer.MIN_VALUE;

    private byte[] bytes = new byte[0];
    private int pos;
    private int length;
    private int hashCode;

    public ByteSlice() {
    }

    void use(byte[] bytes, int pos, int length) {
        use(bytes, pos, length, NOT_CALCULATED);
    }

    void use(byte[] bytes, int pos, int length, int hashCode) {
        this.bytes = bytes;
        this.pos = pos;
        this.length = length;
        this.hashCode = hashCode;
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
    public int hashCode() {
        if (hashCode == NOT_CALCULATED) {
            int hash = 1;
            int end = pos + length;
            for (int i = pos; i < end; i++) {
                hash = (31 * hash) + bytes[i];
            }
            hashCode = hash;
        }
        return hashCode;
    }

    @Override
    public String toString() {
        return new String(bytes, pos, length, StandardCharsets.UTF_8);
    }
}
