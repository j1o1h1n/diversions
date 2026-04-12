package com.helianthi.jscream;

import java.util.Arrays;

public final class PackedIntLists {

    private int[] lists;
    private int head = -1;
    private int used = 0;

    public PackedIntLists(int initialCapacity) {
        lists = new int[initialCapacity];
    }

    public int list() {
        ensureCapacity(used + 1);
        head = head == -1 ? 0 : head + lists[head] + 1;
        lists[head] = 0;
        used++;
        return head;
    }

    public void append(int val) {
        if (head < 0) {
            throw new IllegalStateException("no active list");
        }
        ensureCapacity(used + 1);
        lists[++lists[head] + head] = val;
        used++;
    }

    public int get(int h, int i) {
        int size = size(h);
        if (i < 0 || i >= size) {
            throw new IndexOutOfBoundsException(i);
        }
        return lists[h + i + 1];
    }

    public int size(int h) {
        if (h < 0 || h > head) {
            throw new IndexOutOfBoundsException(h);
        }
        return lists[h];
    }

    public void reset() {
        Arrays.fill(lists, 0);
        head = -1;
        used = 0;
    }

    private void ensureCapacity(int required) {
        if (required <= 0) {
            return;
        }
        if (required <= lists.length) {
            return;
        }

        int newLength = lists.length;
        while (newLength < required) {
            newLength <<= 1;
        }
        lists = Arrays.copyOf(lists, newLength);
    }
}
