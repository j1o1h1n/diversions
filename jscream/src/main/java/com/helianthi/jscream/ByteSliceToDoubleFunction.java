package com.helianthi.jscream;

@FunctionalInterface
public interface ByteSliceToDoubleFunction {
    double applyAsDouble(byte[] bytes, int offset, int length);
}
