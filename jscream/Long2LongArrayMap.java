import java.util.Arrays;

/**
 * Array-backed long->long hash map using separate chaining with index links.
 *
 * Layout:
 * - head[bucket] = first entry index for bucket, or -1
 * - keys[idx], vals[idx] store entry payload
 * - next[idx] is either:
 *   - next entry in chain, or -1 (end)
 *   - next free slot when idx is on free-list
 * - free points to first free slot, or -1 when full
 */
public final class Long2LongArrayMap {
    private static final int NONE = -1;
    private static final byte EMPTY = 0;
    private static final byte USED = 1;

    private final int[] head;
    private final long[] keys;
    private final long[] vals;
    private final int[] next;
    private final byte[] state;

    private int free;
    private int size;

    public Long2LongArrayMap(int buckets, int capacity) {
        if (buckets <= 0) throw new IllegalArgumentException("buckets must be > 0");
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");

        this.head = new int[buckets];
        this.keys = new long[capacity];
        this.vals = new long[capacity];
        this.next = new int[capacity];
        this.state = new byte[capacity];

        Arrays.fill(head, NONE);

        // Build free-list: 0 -> 1 -> 2 -> ... -> capacity-1 -> NONE
        for (int i = 0; i < capacity - 1; i++) {
            next[i] = i + 1;
        }
        next[capacity - 1] = NONE;
        free = 0;
        size = 0;
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return keys.length;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public boolean isFull() {
        return free == NONE;
    }

    public boolean containsKey(long key) {
        return findIndex(key) != NONE;
    }

    public long getOrDefault(long key, long defaultValue) {
        int idx = findIndex(key);
        return idx == NONE ? defaultValue : vals[idx];
    }

    /**
     * Put key->value.
     * @return previous value if key existed, otherwise defaultValueIfAbsent.
     */
    public long put(long key, long value, long defaultValueIfAbsent) {
        int b = bucket(key);
        int idx = head[b];

        while (idx != NONE) {
            if (state[idx] == USED && keys[idx] == key) {
                long prev = vals[idx];
                vals[idx] = value;
                return prev;
            }
            idx = next[idx];
        }

        if (free == NONE) {
            throw new IllegalStateException("Map is full; no free slots");
        }

        // Pop from free-list.
        int newIdx = free;
        free = next[newIdx];

        keys[newIdx] = key;
        vals[newIdx] = value;
        state[newIdx] = USED;

        // Insert at bucket head.
        next[newIdx] = head[b];
        head[b] = newIdx;

        size++;
        return defaultValueIfAbsent;
    }

    /**
     * Remove key.
     * @return previous value if present, otherwise defaultValueIfAbsent.
     */
    public long remove(long key, long defaultValueIfAbsent) {
        int b = bucket(key);
        int prev = NONE;
        int cur = head[b];

        while (cur != NONE) {
            if (state[cur] == USED && keys[cur] == key) {
                long old = vals[cur];

                // Unlink from bucket chain.
                int chainNext = next[cur];
                if (prev == NONE) {
                    head[b] = chainNext;
                } else {
                    next[prev] = chainNext;
                }

                // Push slot onto free-list.
                state[cur] = EMPTY;
                next[cur] = free;
                free = cur;

                size--;
                return old;
            }
            prev = cur;
            cur = next[cur];
        }

        return defaultValueIfAbsent;
    }

    public void clear() {
        Arrays.fill(head, NONE);
        Arrays.fill(state, EMPTY);
        for (int i = 0; i < next.length - 1; i++) {
            next[i] = i + 1;
        }
        next[next.length - 1] = NONE;
        free = next.length == 0 ? NONE : 0;
        size = 0;
    }

    private int findIndex(long key) {
        int idx = head[bucket(key)];
        while (idx != NONE) {
            if (state[idx] == USED && keys[idx] == key) {
                return idx;
            }
            idx = next[idx];
        }
        return NONE;
    }

    private int bucket(long key) {
        long h = mix64(key);
        return (int) Long.remainderUnsigned(h, head.length);
    }

    // 64-bit finalizer style mix for decent spread from long keys.
    private static long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return z ^ (z >>> 33);
    }
}
