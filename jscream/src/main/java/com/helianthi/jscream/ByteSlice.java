package com.helianthi.jscream;

import java.nio.charset.StandardCharsets;

/**
 * Reusable mutable byte slice used by the decoder. The content is overwritten
 * by the next string token.
 */
public final class ByteSlice implements CharSequence {
    private byte[] bytes = new byte[0];
    private int pos;
    private int length;

    ByteSlice() {
    }

    void use(byte[] bytes, int pos, int length) {
        this.bytes = bytes;
        this.pos = pos;
        this.length = length;
    }

    public int pos() {
        return pos;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public char charAt(int index) {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException(index);
        }
        return (char) (bytes[pos + index] & 0xFF);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        if (start < 0 || end < start || end > length) {
            throw new IndexOutOfBoundsException();
        }
        return new String(bytes, pos + start, end - start, StandardCharsets.US_ASCII);
    }

    @Override
    public String toString() {
        return new String(bytes, pos, length, StandardCharsets.US_ASCII);
    }
}
