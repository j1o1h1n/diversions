package com.helianthi.jscream;

public final class JSValue {

    private final JScreamTape parent;

    private char type;
    private long valueRef;

    public JSValue(JScreamTape parent) {
        this.parent = parent;
    }

    JSValue use(char type, long valueRef) {
        this.type = type;
        this.valueRef = valueRef;
        return this;
    }

    public char valueType() {
        return type;
    }

    public short shortValue() {
        requireType(JScreamTape.INT64);
        return (short) valueRef;
    }

    public int intValue() {
        requireType(JScreamTape.INT64);
        return (int) valueRef;
    }

    public long longValue() {
        requireType(JScreamTape.INT64);
        return valueRef;
    }

    public double doubleValue() {
        requireType(JScreamTape.DOUBLE);
        return Double.longBitsToDouble(valueRef);
    }

    public boolean boolValue() {
        requireType(JScreamTape.BOOL);
        return valueRef == 1;
    }

    public ByteSlice stringValue() {
        requireType(JScreamTape.STRING);
        return parent.stringValue((int) valueRef);
    }

    public JSArray arrayValue() {
        requireType(JScreamTape.ARRAY);
        return parent.arrayValue((int) valueRef);
    }

    public JSObject objectValue() {
        requireType(JScreamTape.OBJECT);
        return parent.objectValue((int) valueRef);
    }

    private void requireType(char expected) {
        if (type != expected) {
            throw new IllegalStateException("expected " + expected + " but was " + type);
        }
    }
}
