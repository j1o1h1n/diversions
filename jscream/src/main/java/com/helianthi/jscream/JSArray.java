package com.helianthi.jscream;

public final class JSArray {

    private JScreamTape parent;
    private PackedIntLists packedLists;
    private int tapeHandle = -1;
    private int h = -1;

    public JSArray() {
    }

    JSArray use(JScreamTape parent, PackedIntLists packedLists, int tapeHandle, int listHandle) {
        this.parent = parent;
        this.packedLists = packedLists;
        this.tapeHandle = tapeHandle;
        this.h = listHandle;
        return this;
    }

    public ByteSlice rawValue(ByteSlice target) {
        requireHandle();
        return parent.rawValue(tapeHandle, target);
    }

    public int size() {
        requireHandle();
        return packedLists.size(h);
    }

    public JSValue get(int index, JSValue target) {
        requireHandle();
        return parent.value(packedLists.get(h, index), target);
    }

    public JSValue valueAt(int index, JSValue target) {
        return get(index, target);
    }

    private void requireHandle() {
        if (h < 0) {
            throw new IllegalStateException("array handle is not initialized");
        }
    }
}
