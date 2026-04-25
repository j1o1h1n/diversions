package com.helianthi.jscream;

import java.util.Arrays;

/**
 * Single-threaded stateful JSON builder. The builder is intended to be reused
 * by calling {@link JScreamEncoder#prepare(ByteArrayBuilder)} before each encode.
 */
public final class JScreamBuilder {
    private static final byte TYPE_OBJECT = 1;
    private static final byte TYPE_ARRAY = 2;

    private ByteArrayBuilder out;
    private byte[] containerTypes = new byte[8];
    private int[] itemCounts = new int[8];
    private int depth;
    private boolean rootWritten;
    private boolean expectingValue;

    JScreamBuilder prepare(ByteArrayBuilder output) {
        this.out = output;
        this.depth = 0;
        this.rootWritten = false;
        this.expectingValue = false;
        return this;
    }

    public JScreamBuilder object() {
        beforeValue();
        push(TYPE_OBJECT);
        out.append('{');
        return this;
    }

    public JScreamBuilder array() {
        beforeValue();
        push(TYPE_ARRAY);
        out.append('[');
        return this;
    }

    public JScreamBuilder key(CharSequence key) {
        requireObjectContext();
        if (expectingValue) {
            throw new IllegalStateException("previous key has no value");
        }

        int index = depth - 1;
        if (itemCounts[index] > 0) {
            out.append(',');
        }
        out.appendJsonString(key);
        out.append(':');
        expectingValue = true;
        return this;
    }

    public JScreamBuilder value(CharSequence value) {
        beforeValue();
        out.appendJsonString(value);
        return this;
    }

    public JScreamBuilder value(long value) {
        beforeValue();
        out.appendLong(value);
        return this;
    }

    public JScreamBuilder value(int value) {
        return value((long) value);
    }

    public JScreamBuilder value(double value) {
        beforeValue();
        out.appendDouble(value);
        return this;
    }

    public JScreamBuilder value(boolean value) {
        beforeValue();
        out.append(value ? "true" : "false");
        return this;
    }

    public JScreamBuilder nullValue() {
        beforeValue();
        out.append("null");
        return this;
    }

    public JScreamBuilder done() {
        if (depth == 0) {
            throw new IllegalStateException("no open container");
        }
        if (containerTypes[depth - 1] == TYPE_OBJECT && expectingValue) {
            throw new IllegalStateException("object key has no value");
        }
        byte type = containerTypes[--depth];
        out.append(type == TYPE_OBJECT ? '}' : ']');
        afterValue();
        return this;
    }

    public boolean isComplete() {
        return rootWritten && depth == 0 && !expectingValue;
    }

    private void beforeValue() {
        if (depth == 0) {
            if (rootWritten) {
                throw new IllegalStateException("root value already written");
            }
            rootWritten = true;
            return;
        }

        int index = depth - 1;
        if (containerTypes[index] == TYPE_OBJECT) {
            if (!expectingValue) {
                throw new IllegalStateException("object values require key()");
            }
            itemCounts[index]++;
            expectingValue = false;
            return;
        }

        if (itemCounts[index] > 0) {
            out.append(',');
        }
        itemCounts[index]++;
    }

    private void afterValue() {
        if (depth == 0) {
            return;
        }
        if (containerTypes[depth - 1] == TYPE_OBJECT) {
            expectingValue = false;
        }
    }

    private void requireObjectContext() {
        if (depth == 0 || containerTypes[depth - 1] != TYPE_OBJECT) {
            throw new IllegalStateException("key() requires object context");
        }
    }

    private void push(byte type) {
        if (depth == containerTypes.length) {
            containerTypes = Arrays.copyOf(containerTypes, depth << 1);
            itemCounts = Arrays.copyOf(itemCounts, depth << 1);
        }
        containerTypes[depth] = type;
        itemCounts[depth] = 0;
        depth++;
    }
}
