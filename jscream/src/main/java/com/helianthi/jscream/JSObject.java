package com.helianthi.jscream;

import java.util.function.Consumer;

public final class JSObject {

    private final JScreamTape parent;
    private final PackedIntLists packedLists;
    private final Entry entry;

    private int e = -1;

    public JSObject(JScreamTape parent, PackedIntLists packedLists) {
        this.parent = parent;
        this.packedLists = packedLists;
        this.entry = new Entry();
    }
 
    JSObject use(int e) {
        this.e = e;
        return this;
    }

    public int size() {
        requireEntry();
        return packedLists.size(e);
    }

    public void forEach(Consumer<Entry> consumer) {
        requireEntry();
        for (int i = 0; i < packedLists.size(e); i++) {
            int h = packedLists.get(e, i);
            entry.key = parent.stringValue(h);
            entry.value = parent.value(h + 1);
            consumer.accept(entry);
        }
    }

    private void requireEntry() {
        if (e < 0) {
            throw new IllegalStateException("object handle is not initialized");
        }
    }

    public static class Entry {
        public ByteSlice key;
        public JSValue value;
    }

}
