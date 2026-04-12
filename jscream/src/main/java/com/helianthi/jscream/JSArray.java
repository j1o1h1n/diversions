package com.helianthi.jscream;

import java.util.function.Consumer;

public final class JSArray {

    private final JScreamTape parent;
    private final PackedIntLists packedLists;
    private int h = -1;

    public JSArray(JScreamTape parent, PackedIntLists packedLists) {
        this.parent = parent;
        this.packedLists = packedLists;
    }
 
    JSArray use(int h) {
        this.h = h;
        return this;
    }

    public int size() {
        requireHandle();
        return packedLists.size(h);
    }

    public void forEach(Consumer<JSValue> consumer) {
        requireHandle();
        for (int i = 0; i < packedLists.size(h); i++) {
            consumer.accept(parent.value(packedLists.get(h, i)));
        }
    }

    private void requireHandle() {
        if (h < 0) {
            throw new IllegalStateException("array handle is not initialized");
        }
    }
}
