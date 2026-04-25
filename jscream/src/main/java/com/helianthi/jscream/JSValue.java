package com.helianthi.jscream;

public final class JSValue {

    private JScreamTape parent;
    private char type;
    private long valueRef;

    public JSValue() {
    }

    JSValue use(JScreamTape parent, char type, long valueRef) {
        this.parent = parent;
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

    public ByteSlice stringValue(ByteSlice target) {
        requireType(JScreamTape.STRING);
        requireParent();
        return parent.stringValue((int) valueRef, target);
    }

    public JSArray arrayValue(JSArray target) {
        requireType(JScreamTape.ARRAY);
        requireParent();
        return parent.arrayValue((int) valueRef, target);
    }

    public JSObject objectValue(JSObject target) {
        requireType(JScreamTape.OBJECT);
        requireParent();
        return parent.objectValue((int) valueRef, target);
    }

    private void requireType(char expected) {
        if (type != expected) {
            throw new IllegalStateException("expected " + expected + " but was " + type);
        }
    }

    private void requireParent() {
        if (parent == null) {
            throw new IllegalStateException("value is not initialized");
        }
    }
}
