package com.helianthi.jscream;

import java.util.Arrays;
import java.util.function.IntConsumer;

public final class IntLinkedListArray {

    private static final int NULL = -1;

    private int[] heads;
    private int[] tails;
    private long[] nodes;

    private int head;
    private int nodeHead;

    public IntLinkedListArray(int initialCapacity, int avgLength) {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("initialCapacity must be > 0");
        }
        if (avgLength <= 0) {
            throw new IllegalArgumentException("avgLength must be > 0");
        }
        heads = new int[initialCapacity];
        tails = new int[initialCapacity];
        nodes = new long[initialCapacity * avgLength];
        reset();
    }

    public void reset() {
        Arrays.fill(heads, NULL);
        Arrays.fill(tails, NULL);
        Arrays.fill(nodes, pack(0, NULL));
        head = 0;
        nodeHead = 0;
    }

    public int list() {
        ensureCapacity(head + 1, nodeHead);
        heads[head] = NULL;
        tails[head] = NULL;
        return head++;
    }

    public void append(int h, int value) {
        if (h < 0 || h >= head) {
            throw new IndexOutOfBoundsException(h);
        }

        ensureCapacity(head, nodeHead + 1);

        int nodeIndex = nodeHead++;
        nodes[nodeIndex] = pack(value, NULL);

        if (heads[h] == NULL) {
            heads[h] = nodeIndex;
            tails[h] = nodeIndex;
            return;
        }

        int tailIndex = tails[h];
        nodes[tailIndex] = pack(unpackLeft(nodes[tailIndex]), nodeIndex);
        tails[h] = nodeIndex;
    }

    public void forEach(int h, IntConsumer consumer) {
        if (h < 0 || h >= head) {
            throw new IndexOutOfBoundsException(h);
        }

        int nodeIndex = heads[h];
        while (nodeIndex != NULL) {
            long packed = nodes[nodeIndex];
            consumer.accept(unpackLeft(packed));
            nodeIndex = unpackRight(packed);
        }
    }

    public int get(int h, int index) {
        if (h < 0 || h >= head) {
            throw new IndexOutOfBoundsException(h);
        }
        if (index < 0) {
            throw new IndexOutOfBoundsException(index);
        }

        int nodeIndex = heads[h];
        int currentIndex = 0;
        while (nodeIndex != NULL) {
            long packed = nodes[nodeIndex];
            if (currentIndex == index) {
                return unpackLeft(packed);
            }
            currentIndex++;
            nodeIndex = unpackRight(packed);
        }

        throw new IndexOutOfBoundsException(index);
    }

    private static int unpackLeft(long v) {
        return (int) (v >>> 32);
    }

    private static int unpackRight(long v) {
        return (int) v;
    }

    private static long pack(int left, int right) {
        return ((long) left << 32) | (right & 0xFFFF_FFFFL);
    }

    private void ensureCapacity(int requiredHeads, int requiredNodes) {
        if (requiredHeads > heads.length) {
            int newLength = heads.length;
            while (newLength < requiredHeads) {
                newLength <<= 1;
            }
            int oldLength = heads.length;
            heads = Arrays.copyOf(heads, newLength);
            tails = Arrays.copyOf(tails, newLength);
            Arrays.fill(heads, oldLength, newLength, NULL);
            Arrays.fill(tails, oldLength, newLength, NULL);
        }

        if (requiredNodes > nodes.length) {
            int newLength = nodes.length;
            while (newLength < requiredNodes) {
                newLength <<= 1;
            }
            int oldLength = nodes.length;
            nodes = Arrays.copyOf(nodes, newLength);
            Arrays.fill(nodes, oldLength, newLength, pack(0, NULL));
        }
    }
}
