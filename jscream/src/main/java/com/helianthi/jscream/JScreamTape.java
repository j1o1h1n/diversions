package com.helianthi.jscream;

import java.util.Arrays;

public final class JScreamTape {
    private static final int HASH_NOT_STORED = Integer.MIN_VALUE;

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
    private int[] stringHashes = new int[256];
    private long[] rawRanges = new long[256];
    private long[] stack = new long[256];
    private int[] stackHandles = new int[256];
    private final ByteArrayBuilder decodedString = new ByteArrayBuilder(64);


    private int tapeHead = 0;
    private int stackHead = -1;

    private int state = STATE_NONE;

    private final IntLinkedListArray linkedLists = new IntLinkedListArray(256, 16);
    private final PackedIntLists packedLists = new PackedIntLists(256 * 16);
    private final ByteSliceIntMap objectKeyIndex = new ByteSliceIntMap(this);

    // getters

    public JSValue value(int h, JSValue target) {
         char t = typeOf(h);
         long ref = (t == INT64 || t == DOUBLE || t == BOOL) ? values[h] : h;
         return target.use(this, t, ref);
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

    public ByteSlice stringValue(int h, ByteSlice target) {
        requireType(h, STRING);
        long packed = values[h];
        int pos = unpackLeft(packed);
        int length = unpackRight(packed);
        int hash = stringHashes[h];
        if (length >= 0) {
            if (hash == HASH_NOT_STORED) {
                target.use(bytes, pos, length);
            } else {
                target.use(bytes, pos, length, hash);
            }
            return target;
        }

        decodedString.clear();
        decodeString(pos, -length);
        if (hash == HASH_NOT_STORED) {
            target.use(decodedString.buffer(), 0, decodedString.size());
        } else {
            target.use(decodedString.buffer(), 0, decodedString.size(), hash);
        }
        return target;
    }

    public JSArray arrayValue(int h, JSArray target) {
        requireType(h, ARRAY);
        return target.use(this, packedLists, h, (int) values[h]);
    }

    public JSObject objectValue(int h, JSObject target) {
        requireType(h, OBJECT);
        return target.use(this, packedLists, h, (int) values[h]);
    }

    final ByteSlice keyBuffer = new ByteSlice();

    int objectKeyHandle(int objectEntry, ByteSlice wanted) {
        return objectKeyIndex.get(objectEntry, wanted);
    }

    int indexOfObjectKey(int objectEntry, ByteSlice wanted) {
        int keyHandle = objectKeyHandle(objectEntry, wanted);
        if (keyHandle < 0) {
            return -1;
        }
        int size = packedLists.size(objectEntry);
        for (int i = 0; i < size; i++) {
            if (packedLists.get(objectEntry, i) == keyHandle) {
                return i;
            }
        }
        return -1;
    }

    private boolean contentEquals(ByteSlice left, ByteSlice right) {
        int length = left.length();
        if (length != right.length()) {
            return false;
        }
        byte[] leftBytes = left.bytes();
        byte[] rightBytes = right.bytes();
        int leftPos = left.pos();
        int rightPos = right.pos();
        for (int i = 0; i < length; i++) {
            if (leftBytes[leftPos + i] != rightBytes[rightPos + i]) {
                return false;
            }
        }
        return true;
    }

    public ByteSlice rawValue(int h, ByteSlice target) {
        if (tape[h] != ARRAY && tape[h] != OBJECT) {
            throw new IllegalStateException("expected array/object but was " + tape[h]);
        }
        long packed = rawRanges[h];
        int pos = unpackLeft(packed);
        int length = unpackRight(packed);
        target.use(bytes, pos, length);
        return target;
    }

    public void clear() {
        this.tapeHead = 0;
        this.stackHead = -1;
        this.state = STATE_NONE;
        this.linkedLists.reset();
        this.packedLists.reset();
        this.objectKeyIndex.clear();
        Arrays.fill(this.tape, (char) 0);
        Arrays.fill(this.values, 0L);
        Arrays.fill(this.stringHashes, HASH_NOT_STORED);
        Arrays.fill(this.rawRanges, 0L);
        Arrays.fill(this.stack, 0L);
        Arrays.fill(this.stackHandles, 0);
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
        for (int h = 0; h < tapeHead; h++) {
            if (tape[h] != OBJECT) {
                continue;
            }
            int objectEntry = (int) values[h];
            int size = packedLists.size(objectEntry);
            for (int i = 0; i < size; i++) {
                int keyHandle = packedLists.get(objectEntry, i);
                objectKeyIndex.put(objectEntry, stringValue(keyHandle, keyBuffer), keyHandle);
            }
        }
    }

    void addArray(int start) {
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
        rawRanges[tapeHead] = pack(start, 0);
        stackHandles[stackHead] = tapeHead;
        values[tapeHead] = stack[stackHead] = this.linkedLists.list();
        this.state = STATE_ARRAY;
        tapeHead++;
    }

    void endArray(int end) {
        if (state == STATE_DONE) {
            throw invalidState("cannot end array after close()");
        }
        if (state != STATE_ARRAY) {
            throw invalidState("not in array state");
        }
        int h = stackHandles[stackHead];
        rawRanges[h] = pack(unpackLeft(rawRanges[h]), end - unpackLeft(rawRanges[h]));
        stackHead--;
        this.state = stackHead < 0 ? STATE_NONE : this.tape[this.linkedLists.get((int) stack[stackHead], 0) - 1] == ARRAY ? STATE_ARRAY : STATE_OBJECT_KEY;
    }

    void addObject(int start) {
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
        ensureCapacity(tapeHead + 1, stackHead + 1);
        tape[tapeHead] = OBJECT;
        rawRanges[tapeHead] = pack(start, 0);
        stackHandles[stackHead] = tapeHead;
        values[tapeHead] = stack[stackHead] = this.linkedLists.list();
        this.state = STATE_OBJECT_KEY;
        tapeHead++;
    }

    void endObject(int end) {
        if (state == STATE_DONE) {
            throw invalidState("cannot end object after close()");
        }
        if (state != STATE_OBJECT_KEY) {
            throw invalidState("object is not ready to end");
        }
        int h = stackHandles[stackHead];
        rawRanges[h] = pack(unpackLeft(rawRanges[h]), end - unpackLeft(rawRanges[h]));
        stackHead--;
        this.state = stackHead < 0 ? STATE_NONE : this.tape[this.linkedLists.get((int) stack[stackHead], 0) - 1] == ARRAY ? STATE_ARRAY : STATE_OBJECT_KEY;
    }

    public void addObjectKey(ByteSlice slice, boolean escaped) {
        if (state == STATE_DONE) {
            throw invalidState("cannot add string after close()");
        }
        if (state != STATE_OBJECT_KEY) {
            throw invalidState("object key must be added in object-key state");
        }
        ensureCapacity(tapeHead + 1, stackHead);
        tape[tapeHead] = STRING;
        int length = slice.length();
        values[tapeHead] = pack(slice.pos(), escaped ? -length : length);
        stringHashes[tapeHead] = escaped ? hashCode(slice, true) : slice.hashCode();
        linkedLists.append((int) stack[stackHead], tapeHead);
        state = STATE_OBJECT_VALUE;
        tapeHead++;
    }

    public void addStringValue(ByteSlice slice, boolean escaped) {
        if (state == STATE_DONE) {
            throw invalidState("cannot add string after close()");
        }
        ensureCapacity(tapeHead + 1, stackHead);
        tape[tapeHead] = STRING;
        int length = slice.length();
        values[tapeHead] = pack(slice.pos(), escaped ? -length : length);
        stringHashes[tapeHead] = HASH_NOT_STORED;
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
            int oldLength = tape.length;
            int newLength = oldLength;
            while (newLength < requiredTape) {
                newLength <<= 1;
            }
            tape = Arrays.copyOf(tape, newLength);
            values = Arrays.copyOf(values, newLength);
            stringHashes = Arrays.copyOf(stringHashes, newLength);
            Arrays.fill(stringHashes, oldLength, newLength, HASH_NOT_STORED);
            rawRanges = Arrays.copyOf(rawRanges, newLength);
        }

        int stackRequired = requiredStack + 1;
        if (stackRequired > stack.length) {
            int newLength = stack.length;
            while (newLength < stackRequired) {
                newLength <<= 1;
            }
            stack = Arrays.copyOf(stack, newLength);
            stackHandles = Arrays.copyOf(stackHandles, newLength);
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

    private int hashCode(ByteSlice slice, boolean escaped) {
        if (!escaped) {
            return hashCode(slice.bytes(), slice.pos(), slice.length());
        }

        decodedString.clear();
        decodeString(slice.pos(), slice.length());
        return hashCode(decodedString.buffer(), 0, decodedString.size());
    }

    private static int hashCode(byte[] bytes, int pos, int length) {
        int hash = 1;
        int end = pos + length;
        for (int i = pos; i < end; i++) {
            hash = (31 * hash) + bytes[i];
        }
        return hash;
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
                    int codeUnit = parseUnicodeEscape(i + 1);
                    i += 4;
                    if (Character.isHighSurrogate((char) codeUnit)
                        && i + 6 < end
                        && bytes[i + 1] == '\\'
                        && bytes[i + 2] == 'u') {
                        int low = parseUnicodeEscape(i + 3);
                        if (Character.isLowSurrogate((char) low)) {
                            appendUtf8(Character.toCodePoint((char) codeUnit, (char) low));
                            i += 6;
                            break;
                        }
                    }
                    if (Character.isLowSurrogate((char) codeUnit)) {
                        throw new IllegalArgumentException("unexpected low surrogate");
                    }
                    appendUtf8(codeUnit);
                    break;
                default:
                    throw new IllegalArgumentException("unsupported escape");
            }
        }
    }

    private int parseUnicodeEscape(int pos) {
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
        return value;
    }

    private void appendUtf8(int codePoint) {
        if (codePoint < 0x80) {
            decodedString.appendByte((byte) codePoint);
            return;
        }
        if (codePoint < 0x800) {
            decodedString.appendByte((byte) (0xC0 | (codePoint >>> 6)));
            decodedString.appendByte((byte) (0x80 | (codePoint & 0x3F)));
            return;
        }
        if (codePoint < 0x10000) {
            decodedString.appendByte((byte) (0xE0 | (codePoint >>> 12)));
            decodedString.appendByte((byte) (0x80 | ((codePoint >>> 6) & 0x3F)));
            decodedString.appendByte((byte) (0x80 | (codePoint & 0x3F)));
            return;
        }
        decodedString.appendByte((byte) (0xF0 | (codePoint >>> 18)));
        decodedString.appendByte((byte) (0x80 | ((codePoint >>> 12) & 0x3F)));
        decodedString.appendByte((byte) (0x80 | ((codePoint >>> 6) & 0x3F)));
        decodedString.appendByte((byte) (0x80 | (codePoint & 0x3F)));
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
