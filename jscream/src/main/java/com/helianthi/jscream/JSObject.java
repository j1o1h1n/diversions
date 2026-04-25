package com.helianthi.jscream;

public final class JSObject {

    private JScreamTape parent;
    private PackedIntLists packedLists;
    private int tapeHandle = -1;
    private int e = -1;

    public JSObject() {
    }

    JSObject use(JScreamTape parent, PackedIntLists packedLists, int tapeHandle, int e) {
        this.parent = parent;
        this.packedLists = packedLists;
        this.tapeHandle = tapeHandle;
        this.e = e;
        return this;
    }

    public ByteSlice rawValue(ByteSlice target) {
        requireEntry();
        return parent.rawValue(tapeHandle, target);
    }

    public int size() {
        requireEntry();
        return packedLists.size(e);
    }

    public ByteSlice keyAt(int index, ByteSlice target) {
        requireEntry();
        return parent.stringValue(packedLists.get(e, index), target);
    }

    public boolean containsKey(ByteSlice key) {
        return parent.indexOfObjectKey(e, key) >= 0;
    }

    public JSValue get(ByteSlice key, JSValue target) {
        int index = parent.indexOfObjectKey(e, key);
        return index < 0 ? null : valueAt(index, target);
    }

    public JSValue valueAt(int index, JSValue target) {
        requireEntry();
        return parent.value(packedLists.get(e, index) + 1, target);
    }

    private void requireEntry() {
        if (e < 0) {
            throw new IllegalStateException("object handle is not initialized");
        }
    }
}
