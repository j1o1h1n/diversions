package com.helianthi.jscream;

import java.util.Arrays;

public final class JScreamTape {

    public static final char ARRAY = '[';
    public static final char OBJECT = '{';
    public static final char STRING = '"';
    public static final char INT64 = 'l';
    public static final char DOUBLE = 'd';
    public static final char BOOL = 'b';
    public static final char NULL_VALUE = 'n';

    private static final int STATE_NONE = 0;
    private static final int STATE_ARRAY = 1;
    private static final int STATE_OBJECT_KEY = 2;
    private static final int STATE_OBJECT_VALUE = 3;
    private static final int STATE_DONE = 99;

    private byte[] bytes;

    private char[] tape = new char[256];
    private long[] values = new long[256];
    private long[] stack = new long[256];
    private final ByteArrayBuilder decodedString = new ByteArrayBuilder(64);


    private int tapeHead = 0;
    private int stackHead = -1;

    private int state = STATE_NONE;

    private final IntLinkedListArray linkedLists = new IntLinkedListArray(256, 16);
    private final PackedIntLists packedLists = new PackedIntLists(256 * 16);

    private final ObjectPool<JSValue> valuePool;
    private final ObjectPool<ByteSlice> slicePool;
    private final ObjectPool<JSArray> arrayPool;
    private final ObjectPool<JSObject> objectPool;

    public JScreamTape() {
        this.valuePool = new ObjectPool<>(() -> new JSValue(this));
        this.slicePool = new ObjectPool<>(ByteSlice::new);
        this.arrayPool = new ObjectPool<>(() -> new JSArray(this, packedLists));
        this.objectPool = new ObjectPool<>(() -> new JSObject(this, packedLists));
    }

    // getters

    public JSValue value(int h) {
         JSValue value = valuePool.acquire();
         char t = typeOf(h);
         long ref = (t == INT64 || t == DOUBLE || t == BOOL) ? values[h] : h;
         value.use(t, ref);
         return value;
    }

    public char typeOf(int h) {
        return tape[h];
    }

    public short shortValue(int h) {
        requireType(h, INT64);
        return (short) values[h];
    }

    public int intValue(int h) {
        requireType(h, INT64);
        return (int) values[h];
    }

    public long longValue(int h) {
        requireType(h, INT64);
        return values[h];
    }
    
    public double doubleValue(int h) {
        requireType(h, DOUBLE);
        return Double.longBitsToDouble(values[h]);
    }

    public boolean boolValue(int h) {
        requireType(h, BOOL);
        int val = (int) values[h];
        return val == 1;
    }

    public ByteSlice stringValue(int h) {
        requireType(h, STRING);
        long packed = values[h];
        int pos = unpackLeft(packed);
        int length = unpackRight(packed);
        if (length >= 0) {
            ByteSlice slice = slicePool.acquire();
            slice.use(bytes, pos, length);
            return slice;
        }

        decodedString.clear();
        decodeString(pos, -length);
        ByteSlice slice = slicePool.acquire();
        slice.use(decodedString.buffer(), 0, decodedString.size());
        return slice;
    }

    public JSArray arrayValue(int h) {
        requireType(h, ARRAY);
        return arrayPool.acquire().use((int) values[h]);
    }

    public JSObject objectValue(int h) {
        requireType(h, OBJECT);
        return objectPool.acquire().use((int) values[h]);
    }

    public void clear() {
        this.valuePool.clear();
        this.slicePool.clear();
        this.arrayPool.clear();
        this.objectPool.clear();
        this.bytes = new byte[0];
        this.tapeHead = 0;
        this.stackHead = -1;
        this.state = STATE_NONE;
        this.linkedLists.reset();
        this.packedLists.reset();
        Arrays.fill(this.tape, (char) 0);
        Arrays.fill(this.values, 0L);
        Arrays.fill(this.stack, 0L);
    }

    // builders

    void prepare(byte[] bytes) {
        clear();
        this.bytes = bytes;
    }

    void close() {
        if (state == STATE_DONE) {
            return;
        }
        this.state = STATE_DONE;
        for (int h = 0; h < tapeHead; h++) {
            if (tape[h] != ARRAY && tape[h] != OBJECT) {
                continue;
            }
            int e = (int) values[h];
            values[h] = packedLists.list();
            linkedLists.forEach(e, packedLists::append);
        }
    }

    void addArray() {
        if (state == STATE_DONE) {
            throw invalidState("cannot add array after close()");
        }
        if (state == STATE_OBJECT_KEY) {
            throw invalidState("object key must be a string");
        }
        if (state == STATE_ARRAY) {
            this.linkedLists.append((int) stack[stackHead], tapeHead);
        }
        stackHead++;
        ensureCapacity(tapeHead + 1, stackHead + 1);
        tape[tapeHead] = ARRAY;
        values[tapeHead] = stack[stackHead] = this.linkedLists.list();
        this.state = STATE_ARRAY;
        tapeHead++;
    }

    void endArray() {
        if (state == STATE_DONE) {
            throw invalidState("cannot end array after close()");
        }
        if (state != STATE_ARRAY) {
            throw invalidState("not in array state");
        }
        stackHead--;
        this.state = stackHead < 0 ? STATE_NONE : this.tape[this.linkedLists.get((int) stack[stackHead], 0) - 1] == ARRAY ? STATE_ARRAY : STATE_OBJECT_KEY;
    }

    void addObject() {
        if (state == STATE_DONE) {
            throw invalidState("cannot add object after close()");
        }
        if (state == STATE_OBJECT_KEY) {
            throw invalidState("object key must be a string");
        }
        if (state == STATE_ARRAY) {
            this.linkedLists.append((int) stack[stackHead], tapeHead);
        }
        stackHead++;
        ensureCapacity(tapeHead, stackHead + 1);
        tape[tapeHead] = OBJECT;
        values[tapeHead] = stack[stackHead] = this.linkedLists.list();
        this.state = STATE_OBJECT_KEY;
        tapeHead++;
    }

    void endObject() {
        if (state == STATE_DONE) {
            throw invalidState("cannot end object after close()");
        }
        if (state != STATE_OBJECT_KEY) {
            throw invalidState("object is not ready to end");
        }
        stackHead--;
        this.state = stackHead < 0 ? STATE_NONE : this.tape[this.linkedLists.get((int) stack[stackHead], 0) - 1] == ARRAY ? STATE_ARRAY : STATE_OBJECT_KEY;
    }

    public void addString(ByteSlice slice) {
        if (state == STATE_DONE) {
            throw invalidState("cannot add string after close()");
        }
        ensureCapacity(tapeHead + 1, stackHead);
        tape[tapeHead] = STRING;
        int length = slice.length();
        values[tapeHead] = pack(slice.pos(), containsEscape(slice) ? -length : length);
        if (state == STATE_ARRAY || state == STATE_OBJECT_KEY) {
            linkedLists.append((int) stack[stackHead], tapeHead);
        }
        if (state == STATE_OBJECT_KEY) {
            state = STATE_OBJECT_VALUE;
        } else if (state == STATE_OBJECT_VALUE) {
            state = STATE_OBJECT_KEY;
        }
        tapeHead++;
    }

    public void addInt64(long value) {
        if (state == STATE_DONE) {
            throw invalidState("cannot add int64 after close()");
        }
        if (state == STATE_OBJECT_KEY) {
            throw invalidState("object key must be a string");
        }
        ensureCapacity(tapeHead + 1, stackHead);
        tape[tapeHead] = INT64;
        values[tapeHead] = value;
        if (state == STATE_ARRAY) {
            linkedLists.append((int) stack[stackHead], tapeHead);
        }
        if (state == STATE_OBJECT_VALUE) {
            state = STATE_OBJECT_KEY;
        }
        tapeHead++;
    }

    public void addDouble(double value) {
        if (state == STATE_DONE) {
            throw invalidState("cannot add double after close()");
        }
        if (state == STATE_OBJECT_KEY) {
            throw invalidState("object key must be a string");
        }
        ensureCapacity(tapeHead + 1, stackHead);
        tape[tapeHead] = DOUBLE;
        values[tapeHead] = Double.doubleToRawLongBits(value);
        if (state == STATE_ARRAY) {
            linkedLists.append((int) stack[stackHead], tapeHead);
        }
        if (state == STATE_OBJECT_VALUE) {
            state = STATE_OBJECT_KEY;
        }
        tapeHead++;
    }

    public void addBoolean(boolean value) {
        if (state == STATE_DONE) {
            throw invalidState("cannot add boolean after close()");
        }
        if (state == STATE_OBJECT_KEY) {
            throw invalidState("object key must be a string");
        }
        ensureCapacity(tapeHead + 1, stackHead);
        tape[tapeHead] = BOOL;
        values[tapeHead] = value ? 1 : 0;
        if (state == STATE_ARRAY) {
            linkedLists.append((int) stack[stackHead], tapeHead);
        }
        if (state == STATE_OBJECT_VALUE) {
            state = STATE_OBJECT_KEY;
        }
        tapeHead++;
    }

    public void addNull() {
        if (state == STATE_DONE) {
            throw invalidState("cannot add null after close()");
        }
        if (state == STATE_OBJECT_KEY) {
            throw invalidState("object key must be a string");
        }
        ensureCapacity(tapeHead + 1, stackHead);
        tape[tapeHead] = NULL_VALUE;
        values[tapeHead] = -1;
        if (state == STATE_ARRAY) {
            linkedLists.append((int) stack[stackHead], tapeHead);
        }
        if (state == STATE_OBJECT_VALUE) {
            state = STATE_OBJECT_KEY;
        }
        tapeHead++;
    }

    void ensureCapacity(int requiredTape, int requiredStack) {
        if (requiredTape > tape.length) {
            int newLength = tape.length;
            while (newLength < requiredTape) {
                newLength <<= 1;
            }
            tape = Arrays.copyOf(tape, newLength);
            values = Arrays.copyOf(values, newLength);
        }

        int stackRequired = requiredStack + 1;
        if (stackRequired > stack.length) {
            int newLength = stack.length;
            while (newLength < stackRequired) {
                newLength <<= 1;
            }
            stack = Arrays.copyOf(stack, newLength);
        }
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

    private boolean containsEscape(ByteSlice slice) {
        int pos = slice.pos();
        int end = pos + slice.length();
        for (int i = pos; i < end; i++) {
            if (bytes[i] == '\\') {
                return true;
            }
        }
        return false;
    }

    private void decodeString(int pos, int length) {
        int end = pos + length;
        for (int i = pos; i < end; i++) {
            byte ch = bytes[i];
            if (ch != '\\') {
                decodedString.appendByte(ch);
                continue;
            }

            byte escaped = bytes[++i];
            switch (escaped) {
                case '"':
                case '\\':
                case '/':
                    decodedString.appendByte(escaped);
                    break;
                case 'b':
                    decodedString.appendByte((byte) '\b');
                    break;
                case 'f':
                    decodedString.appendByte((byte) '\f');
                    break;
                case 'n':
                    decodedString.appendByte((byte) '\n');
                    break;
                case 'r':
                    decodedString.appendByte((byte) '\r');
                    break;
                case 't':
                    decodedString.appendByte((byte) '\t');
                    break;
                case 'u':
                    decodedString.appendByte(parseUnicodeEscapeAscii(i + 1));
                    i += 4;
                    break;
                default:
                    throw new IllegalArgumentException("unsupported escape");
            }
        }
    }

    private byte parseUnicodeEscapeAscii(int pos) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            byte ch = bytes[pos + i];
            value <<= 4;
            if (ch >= '0' && ch <= '9') {
                value |= ch - '0';
            } else if (ch >= 'A' && ch <= 'F') {
                value |= 10 + (ch - 'A');
            } else if (ch >= 'a' && ch <= 'f') {
                value |= 10 + (ch - 'a');
            } else {
                throw new IllegalArgumentException("invalid unicode escape");
            }
        }
        if (value > 0x7F) {
            throw new IllegalArgumentException("only ASCII unicode escapes are supported");
        }
        return (byte) value;
    }

    private void requireType(int h, char expected) {
        if (tape[h] != expected) {
            throw new IllegalStateException("expected " + expected + " but was " + tape[h]);
        }
    }

    private IllegalStateException invalidState(String message) {
        return new IllegalStateException(message);
    }
}
