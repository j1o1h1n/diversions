package com.helianthi.jscream;

import java.util.Objects;
import java.nio.charset.StandardCharsets;

public final class JScreamParser {

    private final JScreamDecoder decoder = new JScreamDecoder();
    private final JScreamTape tape = new JScreamTape();
    private final ByteSliceToDoubleFunction doubleParser;

    public JScreamParser() {
        this((bytes, offset, length) -> Double.parseDouble(new String(bytes, offset, length, StandardCharsets.US_ASCII)));
    }

    public JScreamParser(ByteSliceToDoubleFunction doubleParser) {
        this.doubleParser = Objects.requireNonNull(doubleParser, "doubleParser");
    }

    public JSValue parse(ByteArrayBuilder buff, JSValue target) {
        decoder.prepare(buff);
        tape.prepare(buff.buffer());
        for (JSToken token = decoder.next(); token != JSToken.EOF; token = decoder.next()) {
            switch (token) {
                case FIELD_NAME:
                case STRING:
                    tape.addString((ByteSlice) decoder.stringValue());
                    break;
                case LONG:
                    tape.addInt64(decoder.longValue());
                    break;
                case NUMBER:
                    ByteSlice number = decoder.numberValue();
                    tape.addDouble(doubleParser.applyAsDouble(number.bytes(), number.pos(), number.length()));
                    break;
                case START_OBJECT:
                    tape.addObject(decoder.tokenStart());
                    break;
                case END_OBJECT:
                    tape.endObject(decoder.tokenEnd());
                    break;
                case START_ARRAY:
                    tape.addArray(decoder.tokenStart());
                    break;
                case END_ARRAY:
                    tape.endArray(decoder.tokenEnd());
                    break;
                case TRUE:
                    tape.addBoolean(true);
                    break;
                case FALSE:
                    tape.addBoolean(false);
                    break;
                case NULL:
                    tape.addNull();
                    break;
                case EOF:
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected token " + token);
            }
        }
        tape.close();
        return tape.value(0, target);
    }

}
