package com.helianthi.jscream;

import java.util.Objects;
import java.util.function.ToDoubleFunction;

public final class JScreamParser {

    private final JScreamDecoder decoder = new JScreamDecoder();
    private final JScreamTape tape = new JScreamTape();
    private final ToDoubleFunction<ByteSlice> doubleParser;

    public JScreamParser() {
        this(bs -> Double.parseDouble(bs.toString()));
    }

    public JScreamParser(ToDoubleFunction<ByteSlice> doubleParser) {
        this.doubleParser = Objects.requireNonNull(doubleParser, "doubleParser");
    }

    public JSValue parse(ByteArrayBuilder buff) {
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
                    tape.addDouble(doubleParser.applyAsDouble(decoder.numberValue()));
                    break;
                case START_OBJECT:
                    tape.addObject();
                    break;
                case END_OBJECT:
                    tape.endObject();
                    break;
                case START_ARRAY:
                    tape.addArray();
                    break;
                case END_ARRAY:
                    tape.endArray();
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
        return tape.value(0);
    }

}
