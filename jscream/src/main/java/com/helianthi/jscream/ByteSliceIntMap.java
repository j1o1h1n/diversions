package com.helianthi.jscream;

import java.util.Arrays;

final class ByteSliceIntMap {

    private static final long EMPTY = -1L;

    private final JScreamTape parent;
    private final ByteSlice keyBuffer = new ByteSlice();

    private long[] keys = new long[16];
    private int[] values = new int[16];
    private int size;

    ByteSliceIntMap(JScreamTape parent) {
        this.parent = parent;
        Arrays.fill(keys, EMPTY);
    }

    void put(int objectEntry, ByteSlice key, int keyHandle) {
        ensureCapacity();
        insert(objectEntry, key.hashCode(), key, keyHandle);
    }

    int get(int objectEntry, ByteSlice key) {
        int hash = key.hashCode();
        int slot = probeStart(objectEntry, hash, keys.length);
        while (true) {
            long packedKey = keys[slot];
            if (packedKey == EMPTY) {
                return -1;
            }
            if (unpackLeft(packedKey) == objectEntry
                && unpackRight(packedKey) == hash
                && keyEquals(values[slot], key)) {
                return values[slot];
            }
            slot = (slot + 1) & (keys.length - 1);
        }
    }

    void clear() {
        Arrays.fill(keys, EMPTY);
        Arrays.fill(values, 0);
        size = 0;
    }

    private void ensureCapacity() {
        if ((size + 1) * 2 <= keys.length) {
            return;
        }

        long[] oldKeys = keys;
        int[] oldValues = values;
        keys = new long[oldKeys.length << 1];
        values = new int[oldValues.length << 1];
        Arrays.fill(keys, EMPTY);
        size = 0;
        for (int i = 0; i < oldKeys.length; i++) {
            long packedKey = oldKeys[i];
            if (packedKey == EMPTY) {
                continue;
            }
            reinsert(unpackLeft(packedKey), unpackRight(packedKey), oldValues[i]);
        }
    }

    private void reinsert(int objectEntry, int hash, int keyHandle) {
        int slot = probeStart(objectEntry, hash, keys.length);
        while (keys[slot] != EMPTY) {
            slot = (slot + 1) & (keys.length - 1);
        }
        keys[slot] = pack(objectEntry, hash);
        values[slot] = keyHandle;
        size++;
    }

    private void insert(int objectEntry, int hash, ByteSlice key, int keyHandle) {
        int slot = probeStart(objectEntry, hash, keys.length);
        while (true) {
            long packedKey = keys[slot];
            if (packedKey == EMPTY) {
                keys[slot] = pack(objectEntry, hash);
                values[slot] = keyHandle;
                size++;
                return;
            }
            if (unpackLeft(packedKey) == objectEntry
                && unpackRight(packedKey) == hash
                && keyEquals(values[slot], key)) {
                values[slot] = keyHandle;
                return;
            }
            slot = (slot + 1) & (keys.length - 1);
        }
    }

    private int probeStart(int objectEntry, int hash, int length) {
        return ((31 * hash) + objectEntry) & (length - 1);
    }

    private boolean keyEquals(int keyHandle, ByteSlice wanted) {
        parent.stringValue(keyHandle, keyBuffer);
        int length = keyBuffer.length();
        if (length != wanted.length()) {
            return false;
        }
        byte[] leftBytes = keyBuffer.bytes();
        byte[] rightBytes = wanted.bytes();
        int leftPos = keyBuffer.pos();
        int rightPos = wanted.pos();
        for (int i = 0; i < length; i++) {
            if (leftBytes[leftPos + i] != rightBytes[rightPos + i]) {
                return false;
            }
        }
        return true;
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
}
