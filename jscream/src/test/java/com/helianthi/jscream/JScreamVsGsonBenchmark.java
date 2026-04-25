package com.helianthi.jscream;

import ch.randelshofer.fastdoubleparser.JavaDoubleParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public final class JScreamVsGsonBenchmark {
    private static final String[] DEFAULT_FIXTURES = {
        "json/canada.json",
        "json/citm_catalog.json",
        "json/rfc-example-object.json"
    };

    private static volatile int sink;

    private JScreamVsGsonBenchmark() {
    }

    public static void main(String[] args) {
        String[] fixtures = args.length == 0 ? DEFAULT_FIXTURES : args;
        for (String fixture : fixtures) {
            runFixture(fixture);
        }
    }

    private static void runFixture(String fixture) {
        byte[] bytes = readResource(fixture);
        int warmupIterations = fixture.endsWith("rfc-example-object.json") ? 20_000 : 200;
        int measureIterations = fixture.endsWith("rfc-example-object.json") ? 50_000 : 500;

        BenchmarkResult jscream = benchmarkJscream(bytes, warmupIterations, measureIterations);
        BenchmarkResult gson = benchmarkGson(bytes, warmupIterations, measureIterations);

        System.out.printf("%n%s (%d bytes)%n", fixture, bytes.length);
        System.out.printf("  jscream  ns/obj=%.1f  throughput=%.1f ops/s%n",
            jscream.avgNanos(), jscream.opsPerSecond());
        System.out.printf("  gson     ns/obj=%.1f  throughput=%.1f ops/s%n",
            gson.avgNanos(), gson.opsPerSecond());
        System.out.printf("  ratio    %.2fx%n", gson.avgNanos() / jscream.avgNanos());
    }

    private static BenchmarkResult benchmarkJscream(byte[] bytes, int warmupIterations, int measureIterations) {
        ByteArrayBuilder buffer = new ByteArrayBuilder(bytes.length);
        for (byte b : bytes) {
            buffer.appendByte(b);
        }
        JScreamParser parser = new JScreamParser(JavaDoubleParser::parseDouble);
        JSValue root = new JSValue();
        JSObject object = new JSObject();
        JSArray array = new JSArray();

        for (int i = 0; i < warmupIterations; i++) {
            sink ^= inspectJscream(parser.parse(buffer, root), object, array);
        }

        long start = System.nanoTime();
        for (int i = 0; i < measureIterations; i++) {
            sink ^= inspectJscream(parser.parse(buffer, root), object, array);
        }
        return new BenchmarkResult(System.nanoTime() - start, measureIterations);
    }

    private static BenchmarkResult benchmarkGson(byte[] bytes, int warmupIterations, int measureIterations) {
        for (int i = 0; i < warmupIterations; i++) {
            sink ^= inspectGson(parseGson(bytes));
        }

        long start = System.nanoTime();
        for (int i = 0; i < measureIterations; i++) {
            sink ^= inspectGson(parseGson(bytes));
        }
        return new BenchmarkResult(System.nanoTime() - start, measureIterations);
    }

    private static JsonElement parseGson(byte[] bytes) {
        try (InputStreamReader reader = new InputStreamReader(
            new ByteArrayInputStream(bytes), StandardCharsets.UTF_8)) {
            return JsonParser.parseReader(reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static int inspectJscream(JSValue root, JSObject object, JSArray array) {
        if (root.valueType() == JScreamTape.OBJECT) {
            return root.objectValue(object).size();
        }
        if (root.valueType() == JScreamTape.ARRAY) {
            return root.arrayValue(array).size();
        }
        return root.valueType();
    }

    private static int inspectGson(JsonElement root) {
        if (root.isJsonObject()) {
            return root.getAsJsonObject().size();
        }
        if (root.isJsonArray()) {
            return root.getAsJsonArray().size();
        }
        return root.toString().length();
    }

    private static byte[] readResource(String resourceName) {
        try (InputStream in = JScreamVsGsonBenchmark.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (in == null) {
                throw new IllegalArgumentException("missing resource " + resourceName);
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final class BenchmarkResult {
        private final long elapsedNanos;
        private final int iterations;

        private BenchmarkResult(long elapsedNanos, int iterations) {
            this.elapsedNanos = elapsedNanos;
            this.iterations = iterations;
        }

        private double avgNanos() {
            return elapsedNanos / (double) iterations;
        }

        private double opsPerSecond() {
            return iterations / (elapsedNanos / 1_000_000_000.0);
        }
    }
}
