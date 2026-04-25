package com.helianthi.jscream;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Minimal pull-style JSON decoder with reusable views.
 */
public final class JScreamDecoder {
    private static final byte TYPE_OBJECT = 1;
    private static final byte TYPE_ARRAY = 2;

    private static final byte STATE_OBJECT_KEY_OR_END = 1;
    private static final byte STATE_OBJECT_VALUE = 2;
    private static final byte STATE_OBJECT_COMMA_OR_END = 3;
    private static final byte STATE_OBJECT_KEY = 4;
    private static final byte STATE_ARRAY_VALUE_OR_END = 5;
    private static final byte STATE_ARRAY_COMMA_OR_END = 6;
    private static final byte STATE_ARRAY_VALUE = 7;

    private final ByteSlice stringView = new ByteSlice();
    private final ByteSlice numberView = new ByteSlice();

    private byte[] input = new byte[0];
    private int limit;
    private int position;

    private byte[] containerTypes = new byte[8];
    private byte[] states = new byte[8];
    private int depth;
    private boolean rootComplete;

    private JSToken token = JSToken.EOF;
    private long longValue;
    private int numberStart;
    private int numberLength;
    private int tokenStart;
    private int tokenEnd;

    public JScreamDecoder prepare(ByteArrayBuilder source) {
        return prepare(source.buffer(), source.size());
    }

    public JScreamDecoder prepare(byte[] bytes, int length) {
        if (length < 0 || length > bytes.length) {
            throw new IllegalArgumentException("invalid length");
        }
        this.input = bytes;
        this.limit = length;
        this.position = 0;
        this.depth = 0;
        this.rootComplete = false;
        this.token = JSToken.EOF;
        this.longValue = 0L;
        this.numberStart = 0;
        this.numberLength = 0;
        this.tokenStart = 0;
        this.tokenEnd = 0;
        return this;
    }

    public JSToken next() {
        while (true) {
            skipWhitespace();
            if (depth == 0) {
                if (rootComplete) {
                    return token = JSToken.EOF;
                }
                token = parseValueToken();
                if (depth == 0 && isScalarToken(token)) {
                    rootComplete = true;
                }
                return token;
            }

            byte type = containerTypes[depth - 1];
            byte state = states[depth - 1];

            if (type == TYPE_OBJECT) {
                if (state == STATE_OBJECT_VALUE) {
                    token = parseValueToken();
                    if (isScalarToken(token)) {
                        states[depth - 1] = STATE_OBJECT_COMMA_OR_END;
                    }
                    return token;
                }

                if (state == STATE_OBJECT_COMMA_OR_END) {
                    if (tryConsume(',')) {
                        states[depth - 1] = STATE_OBJECT_KEY;
                        continue;
                    }
                    tokenStart = position;
                    if (tryConsume('}')) {
                        tokenEnd = position;
                        depth--;
                        markParentValueComplete();
                        return token = JSToken.END_OBJECT;
                    }
                    throw error("expected ',' or '}'");
                }

                if (state == STATE_OBJECT_KEY) {
                    tokenStart = position;
                    parseString();
                    tokenEnd = position;
                    skipWhitespace();
                    expect(':');
                    states[depth - 1] = STATE_OBJECT_VALUE;
                    return token = JSToken.FIELD_NAME;
                }

                tokenStart = position;
                if (tryConsume('}')) {
                    tokenEnd = position;
                    depth--;
                    markParentValueComplete();
                    return token = JSToken.END_OBJECT;
                }
                tokenStart = position;
                parseString();
                tokenEnd = position;
                skipWhitespace();
                expect(':');
                states[depth - 1] = STATE_OBJECT_VALUE;
                return token = JSToken.FIELD_NAME;
            }

            if (state == STATE_ARRAY_COMMA_OR_END) {
                if (tryConsume(',')) {
                    states[depth - 1] = STATE_ARRAY_VALUE;
                    continue;
                }
                tokenStart = position;
                if (tryConsume(']')) {
                    tokenEnd = position;
                    depth--;
                    markParentValueComplete();
                    return token = JSToken.END_ARRAY;
                }
                throw error("expected ',' or ']'");
            }

            if (state == STATE_ARRAY_VALUE) {
                token = parseValueToken();
                if (depth > 0 && containerTypes[depth - 1] == TYPE_ARRAY && isScalarToken(token)) {
                    states[depth - 1] = STATE_ARRAY_COMMA_OR_END;
                }
                return token;
            }

            tokenStart = position;
            if (tryConsume(']')) {
                tokenEnd = position;
                depth--;
                markParentValueComplete();
                return token = JSToken.END_ARRAY;
            }
            token = parseValueToken();
            if (depth > 0 && containerTypes[depth - 1] == TYPE_ARRAY && isScalarToken(token)) {
                states[depth - 1] = STATE_ARRAY_COMMA_OR_END;
            }
            return token;
        }
    }

    public JSToken token() {
        return token;
    }

    public int tokenStart() {
        return tokenStart;
    }

    public int tokenEnd() {
        return tokenEnd;
    }

    public long longValue() {
        if (token != JSToken.LONG) {
            throw new IllegalStateException("current token is not LONG");
        }
        return longValue;
    }

    public ByteSlice stringValue() {
        if (token != JSToken.FIELD_NAME && token != JSToken.STRING) {
            throw new IllegalStateException("current token is not a string token");
        }
        return stringView;
    }

    public CharSequence numberText() {
        if (token != JSToken.NUMBER) {
            throw new IllegalStateException("current token is not NUMBER");
        }
        return new String(input, numberStart, numberLength, StandardCharsets.US_ASCII);
    }

    public ByteSlice numberValue() {
        if (token != JSToken.NUMBER) {
            throw new IllegalStateException("current token is not NUMBER");
        }
        return numberView;
    }

    private JSToken parseValueToken() {
        if (position >= limit) {
            throw error("unexpected end of input");
        }
        tokenStart = position;
        byte ch = input[position];
        switch (ch) {
            case '{':
                position++;
                tokenEnd = position;
                push(TYPE_OBJECT, STATE_OBJECT_KEY_OR_END);
                return JSToken.START_OBJECT;
            case '[':
                position++;
                tokenEnd = position;
                push(TYPE_ARRAY, STATE_ARRAY_VALUE_OR_END);
                return JSToken.START_ARRAY;
            case '"':
                parseString();
                tokenEnd = position;
                markParentValueComplete();
                return JSToken.STRING;
            case 't':
                expectLiteral("true");
                tokenEnd = position;
                markParentValueComplete();
                return JSToken.TRUE;
            case 'f':
                expectLiteral("false");
                tokenEnd = position;
                markParentValueComplete();
                return JSToken.FALSE;
            case 'n':
                expectLiteral("null");
                tokenEnd = position;
                markParentValueComplete();
                return JSToken.NULL;
            default:
                if (ch == '-' || isDigit(ch)) {
                    JSToken numeric = parseNumber();
                    tokenEnd = position;
                    markParentValueComplete();
                    return numeric;
                }
                throw error("unexpected token");
        }
    }

    int hash;
    private void parseString() {
        expect('"');
        int start = position;
        hash = 1;
        while (position < limit) {
            byte ch = input[position++];
            if (ch == '"') {
                stringView.use(input, start, (position - 1) - start, hash);
                return;
            }
            if (ch == '\\') {
                validateEscape();
                continue;
            }
            if ((ch & 0xFF) < 0x20) {
                throw error("control characters are not allowed in strings");
            }
            hash = (31 * hash) + ch;
        }
        throw error("unterminated string");
    }

    private void validateEscape() {
        if (position >= limit) {
            throw error("incomplete escape");
        }
        byte escaped = input[position++];
        switch (escaped) {
            case '"':
            case '\\':
            case '/':
            case 'b':
            case 'f':
            case 'n':
            case 'r':
            case 't':
                return;
            case 'u':
                parseUnicodeEscape();
                return;
            default:
                throw error("unsupported escape");
        }
    }

    private void parseUnicodeEscape() {
        if (position + 4 > limit) {
            throw error("incomplete unicode escape");
        }
        for (int i = 0; i < 4; i++) {
            byte ch = input[position++];
            if (ch >= '0' && ch <= '9') {
            } else if (ch >= 'A' && ch <= 'F') {
            } else if (ch >= 'a' && ch <= 'f') {
            } else {
                throw error("invalid unicode escape");
            }
        }
    }

    private JSToken parseNumber() {
        int start = position;
        if (input[position] == '-') {
            position++;
        }
        consumeDigits();
        boolean integer = true;
        if (position < limit && input[position] == '.') {
            integer = false;
            position++;
            consumeDigits();
        }
        if (position < limit && (input[position] == 'e' || input[position] == 'E')) {
            integer = false;
            position++;
            if (position < limit && (input[position] == '+' || input[position] == '-')) {
                position++;
            }
            consumeDigits();
        }

        numberStart = start;
        numberLength = position - start;
        numberView.use(input, numberStart, numberLength);
        if (!integer) {
            return JSToken.NUMBER;
        }
        longValue = parseLongAscii(start, numberLength);
        return JSToken.LONG;
    }

    private long parseLongAscii(int start, int len) {
        int index = start;
        boolean negative = false;
        if (input[index] == '-') {
            negative = true;
            index++;
        }
        long value = 0L;
        for (; index < start + len; index++) {
            value = (value * 10L) + (input[index] - '0');
        }
        return negative ? -value : value;
    }

    private void consumeDigits() {
        int begin = position;
        while (position < limit && isDigit(input[position])) {
            position++;
        }
        if (position == begin) {
            throw error("expected digit");
        }
    }

    private void markParentValueComplete() {
        if (depth == 0) {
            rootComplete = true;
            return;
        }
        if (containerTypes[depth - 1] == TYPE_ARRAY) {
            states[depth - 1] = STATE_ARRAY_COMMA_OR_END;
            return;
        }
        states[depth - 1] = STATE_OBJECT_COMMA_OR_END;
    }

    private void push(byte type, byte state) {
        if (depth == containerTypes.length) {
            containerTypes = Arrays.copyOf(containerTypes, depth << 1);
            states = Arrays.copyOf(states, depth << 1);
        }
        containerTypes[depth] = type;
        states[depth] = state;
        depth++;
    }

    private boolean isScalarToken(JSToken current) {
        return current == JSToken.STRING
            || current == JSToken.LONG
            || current == JSToken.NUMBER
            || current == JSToken.TRUE
            || current == JSToken.FALSE
            || current == JSToken.NULL;
    }

    private void skipWhitespace() {
        while (position < limit) {
            byte ch = input[position];
            if (ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t') {
                position++;
                continue;
            }
            return;
        }
    }

    private void expectLiteral(String literal) {
        for (int i = 0; i < literal.length(); i++) {
            expect(literal.charAt(i));
        }
    }

    private void expect(char expected) {
        if (!tryConsume(expected)) {
            throw error("expected '" + expected + "'");
        }
    }

    private boolean tryConsume(char expected) {
        if (position < limit && input[position] == (byte) expected) {
            position++;
            return true;
        }
        return false;
    }

    private boolean isDigit(byte value) {
        return value >= '0' && value <= '9';
    }

    private IllegalArgumentException error(String message) {
        return new IllegalArgumentException(message + " at byte " + position);
    }
}
