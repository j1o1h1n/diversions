package com.helianthi.jscream;

import java.util.Arrays;

/**
 * Reusable ASCII byte buffer with a CharSequence view.
 *
 * <p>The encoder escapes non-ASCII content as {@code \\uXXXX}, so the resulting
 * JSON is always ASCII and this builder can act as a CharSequence without
 * decoding work or per-call allocations.</p>
 */
public final class ByteArrayBuilder implements CharSequence {
    private byte[] bytes;
    private int length;

    public ByteArrayBuilder(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("initialCapacity must be >= 0");
        }
        this.bytes = new byte[Math.max(16, initialCapacity)];
    }

    public void clear() {
        length = 0;
    }

    public int length() {
        return length;
    }

    @Override
    public char charAt(int index) {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException(index);
        }
        return (char) (bytes[index] & 0xFF);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        if (start < 0 || end < start || end > length) {
            throw new IndexOutOfBoundsException();
        }
        return new String(bytes, start, end - start);
    }

    public CharSequence toCharSequence() {
        return this;
    }

    public byte[] buffer() {
        return bytes;
    }

    public int size() {
        return length;
    }

    public void appendByte(byte value) {
        ensureCapacity(length + 1);
        bytes[length++] = value;
    }

    public void append(char value) {
        if (value > 0x7F) {
            throw new IllegalArgumentException("non-ASCII char: " + value);
        }
        appendByte((byte) value);
    }

    public void append(CharSequence value) {
        int len = value.length();
        ensureCapacity(length + len);
        for (int i = 0; i < len; i++) {
            char ch = value.charAt(i);
            if (ch > 0x7F) {
                throw new IllegalArgumentException("non-ASCII char: " + ch);
            }
            bytes[length++] = (byte) ch;
        }
    }

    public void appendLong(long value) {
        if (value == 0) {
            append('0');
            return;
        }

        long current = value;
        if (current < 0) {
            append('-');
            if (current == Long.MIN_VALUE) {
                append("9223372036854775808");
                return;
            }
            current = -current;
        }

        int start = length;
        ensureCapacity(length + 20);
        while (current > 0) {
            long next = current / 10;
            int digit = (int) (current - (next * 10));
            bytes[length++] = (byte) ('0' + digit);
            current = next;
        }
        reverse(start, length - 1);
    }

    public void appendDouble(double value) {
        append(Double.toString(value));
    }

    public void appendJsonString(CharSequence value) {
        append('"');
        int len = value.length();
        for (int i = 0; i < len; i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '"':
                    append("\\\"");
                    break;
                case '\\':
                    append("\\\\");
                    break;
                case '\b':
                    append("\\b");
                    break;
                case '\f':
                    append("\\f");
                    break;
                case '\n':
                    append("\\n");
                    break;
                case '\r':
                    append("\\r");
                    break;
                case '\t':
                    append("\\t");
                    break;
                default:
                    if (ch < 0x20 || ch > 0x7F) {
                        appendUnicodeEscape(ch);
                    } else {
                        append(ch);
                    }
            }
        }
        append('"');
    }

    @Override
    public String toString() {
        return new String(bytes, 0, length);
    }

    private void appendUnicodeEscape(char ch) {
        append('\\');
        append('u');
        appendHex((ch >>> 12) & 0xF);
        appendHex((ch >>> 8) & 0xF);
        appendHex((ch >>> 4) & 0xF);
        appendHex(ch & 0xF);
    }

    private void appendHex(int value) {
        append((char) (value < 10 ? '0' + value : 'A' + (value - 10)));
    }

    private void ensureCapacity(int wanted) {
        if (wanted <= bytes.length) {
            return;
        }
        int next = bytes.length;
        while (next < wanted) {
            next <<= 1;
        }
        bytes = Arrays.copyOf(bytes, next);
    }

    private void reverse(int left, int right) {
        while (left < right) {
            byte tmp = bytes[left];
            bytes[left] = bytes[right];
            bytes[right] = tmp;
            left++;
            right--;
        }
    }
}
