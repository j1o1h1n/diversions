package com.helianthi.jscream;

import ch.randelshofer.fastdoubleparser.JavaDoubleParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class JScreamVsGsonJmhBenchmark {

    @State(Scope.Thread)
    public static class FixtureState {
        @Param({
            "json/rfc-example-object.json",
            "json/canada.json",
            "json/citm_catalog.json"
        })
        public String fixture;

        public byte[] bytes;
        public ByteArrayBuilder buffer;
        public JScreamParser jscreamParser;
        public JSValue root;
        public JSObject object;
        public JSArray array;

        @Setup(Level.Trial)
        public void setUp() {
            bytes = readResource(fixture);
            buffer = new ByteArrayBuilder(bytes.length);
            for (byte b : bytes) {
                buffer.appendByte(b);
            }
            jscreamParser = new JScreamParser(JavaDoubleParser::parseDouble);
            root = new JSValue();
            object = new JSObject();
            array = new JSArray();
        }
    }

    @State(Scope.Thread)
    public static class RfcObjectState {
        public byte[] bytes;
        public ByteArrayBuilder buffer;
        public JScreamParser jscreamParser;
        public JSValue root;
        public JSObject object;
        public JSObject image;
        public JSObject thumbnail;
        public JSArray ids;
        public ByteSlice string;
        public ByteSlice imageKey;
        public ByteSlice widthKey;
        public ByteSlice heightKey;
        public ByteSlice titleKey;
        public ByteSlice thumbnailKey;
        public ByteSlice urlKey;
        public ByteSlice animatedKey;
        public ByteSlice idsKey;

        @Setup(Level.Trial)
        public void setUp() {
            bytes = readResource("json/rfc-example-object.json");
            buffer = new ByteArrayBuilder(bytes.length);
            for (byte b : bytes) {
                buffer.appendByte(b);
            }
            jscreamParser = new JScreamParser(JavaDoubleParser::parseDouble);
            root = new JSValue();
            object = new JSObject();
            image = new JSObject();
            thumbnail = new JSObject();
            ids = new JSArray();
            string = new ByteSlice();
            imageKey = asciiKey("Image");
            widthKey = asciiKey("Width");
            heightKey = asciiKey("Height");
            titleKey = asciiKey("Title");
            thumbnailKey = asciiKey("Thumbnail");
            urlKey = asciiKey("Url");
            animatedKey = asciiKey("Animated");
            idsKey = asciiKey("IDs");
        }
    }

    @Benchmark
    public void parseWithJscream(FixtureState state, Blackhole blackhole) {
        JSValue parsed = state.jscreamParser.parse(state.buffer, state.root);
        if (parsed.valueType() == JScreamTape.OBJECT) {
            blackhole.consume(parsed.objectValue(state.object).size());
            return;
        }
        if (parsed.valueType() == JScreamTape.ARRAY) {
            blackhole.consume(parsed.arrayValue(state.array).size());
            return;
        }
        blackhole.consume(parsed.valueType());
    }

    @Benchmark
    public void parseWithGson(FixtureState state, Blackhole blackhole) {
        JsonElement root = parseGson(state.bytes);
        if (root.isJsonObject()) {
            blackhole.consume(root.getAsJsonObject().size());
            return;
        }
        if (root.isJsonArray()) {
            blackhole.consume(root.getAsJsonArray().size());
            return;
        }
        blackhole.consume(root.toString().length());
    }

    @Benchmark
    public void rfcLookupHashWithJscream(RfcObjectState state, Blackhole blackhole) {
        JSObject rootObject = state.jscreamParser.parse(state.buffer, state.root).objectValue(state.object);
        JSObject image = rootObject.get(state.imageKey, new JSValue()).objectValue(state.image);
        JSObject thumbnail = image.get(state.thumbnailKey, new JSValue()).objectValue(state.thumbnail);
        JSArray ids = image.get(state.idsKey, new JSValue()).arrayValue(state.ids);

        int hash = 1;
        hash = (31 * hash) + Long.hashCode(image.get(state.widthKey, new JSValue()).longValue());
        hash = (31 * hash) + Long.hashCode(image.get(state.heightKey, new JSValue()).longValue());
        hash = (31 * hash) + image.get(state.titleKey, new JSValue()).stringValue(state.string).toString().hashCode();
        hash = (31 * hash) + thumbnail.get(state.urlKey, new JSValue()).stringValue(state.string).toString().hashCode();
        hash = (31 * hash) + Long.hashCode(thumbnail.get(state.heightKey, new JSValue()).longValue());
        hash = (31 * hash) + Long.hashCode(thumbnail.get(state.widthKey, new JSValue()).longValue());
        hash = (31 * hash) + Boolean.hashCode(image.get(state.animatedKey, new JSValue()).boolValue());
        hash = (31 * hash) + Long.hashCode(ids.valueAt(0, new JSValue()).longValue());
        hash = (31 * hash) + Long.hashCode(ids.valueAt(1, new JSValue()).longValue());
        hash = (31 * hash) + Long.hashCode(ids.valueAt(2, new JSValue()).longValue());
        hash = (31 * hash) + Long.hashCode(ids.valueAt(3, new JSValue()).longValue());

        blackhole.consume(hash);
    }

    @Benchmark
    public void rfcLookupHashWithGson(RfcObjectState state, Blackhole blackhole) {
        JsonObject root = parseGson(state.bytes).getAsJsonObject();
        JsonObject image = root.getAsJsonObject("Image");
        JsonObject thumbnail = image.getAsJsonObject("Thumbnail");
        JsonArray ids = image.getAsJsonArray("IDs");

        int hash = 1;
        hash = (31 * hash) + Long.hashCode(image.get("Width").getAsLong());
        hash = (31 * hash) + Long.hashCode(image.get("Height").getAsLong());
        hash = (31 * hash) + image.get("Title").getAsString().hashCode();
        hash = (31 * hash) + thumbnail.get("Url").getAsString().hashCode();
        hash = (31 * hash) + Long.hashCode(thumbnail.get("Height").getAsLong());
        hash = (31 * hash) + Long.hashCode(thumbnail.get("Width").getAsLong());
        hash = (31 * hash) + Boolean.hashCode(image.get("Animated").getAsBoolean());
        hash = (31 * hash) + Long.hashCode(ids.get(0).getAsLong());
        hash = (31 * hash) + Long.hashCode(ids.get(1).getAsLong());
        hash = (31 * hash) + Long.hashCode(ids.get(2).getAsLong());
        hash = (31 * hash) + Long.hashCode(ids.get(3).getAsLong());

        blackhole.consume(hash);
    }

    private static JsonElement parseGson(byte[] bytes) {
        try (InputStreamReader reader = new InputStreamReader(
            new ByteArrayInputStream(bytes), StandardCharsets.UTF_8)) {
            return JsonParser.parseReader(reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static byte[] readResource(String resourceName) {
        try (InputStream in = JScreamVsGsonJmhBenchmark.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (in == null) {
                throw new IllegalArgumentException("missing resource " + resourceName);
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ByteSlice asciiKey(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
        ByteSlice key = new ByteSlice();
        key.use(bytes, 0, bytes.length);
        return key;
    }
}
