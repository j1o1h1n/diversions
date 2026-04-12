package com.helianthi.jscream;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Objects;
import java.util.function.Supplier;

public final class ObjectPool<V> {
    private final Supplier<? extends V> factory;
    private final ArrayDeque<V> pool = new ArrayDeque<>();
    private final ArrayList<V> loaned = new ArrayList<>();

    public ObjectPool(Supplier<? extends V> factory) {
        this.factory = Objects.requireNonNull(factory, "factory");
    }

    public V acquire() {
        V value = pool.pollFirst();
        if (value != null) {
            loaned.add(value);
            return value;
        }
        V created = factory.get();

        if (created == null) {
            throw new IllegalStateException("factory returned null");
        }
        loaned.add(created);
        return created;
    }

    public void release(V value) {
        V checked = Objects.requireNonNull(value, "value");
        loaned.remove(checked);
        pool.addFirst(checked);
    }

    public void clear() {
        for (int i = loaned.size() - 1; i >= 0; i--) {
            pool.addFirst(loaned.get(i));
        }
        loaned.clear();
    }
}
