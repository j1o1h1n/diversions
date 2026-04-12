package com.helianthi.jscream;

public final class JScreamEncoder {
    private final JScreamBuilder builder = new JScreamBuilder();

    public JScreamBuilder prepare(ByteArrayBuilder output) {
        output.clear();
        return builder.prepare(output);
    }
}
