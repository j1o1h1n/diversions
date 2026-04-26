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

    @State(Scope.Thread)
    public static class CitmCatalogState {
        public byte[] bytes;
        public ByteArrayBuilder buffer;
        public JScreamParser jscreamParser;
        public JSValue root;
        public JSObject catalog;
        public JSObject events;
        public JSArray performances;
        public JSArray prices;
        public JSObject performance;
        public JSObject event;
        public JSObject price;
        public JSValue value;
        public ByteSlice string;
        public ByteSlice eventsKey;
        public ByteSlice performancesKey;
        public ByteSlice eventIdKey;
        public ByteSlice pricesKey;
        public ByteSlice amountKey;
        public ByteSlice nameKey;

        @Setup(Level.Trial)
        public void setUp() {
            bytes = readResource("json/citm_catalog.json");
            buffer = new ByteArrayBuilder(bytes.length);
            for (byte b : bytes) {
                buffer.appendByte(b);
            }
            jscreamParser = new JScreamParser(JavaDoubleParser::parseDouble);
            root = new JSValue();
            catalog = new JSObject();
            events = new JSObject();
            performances = new JSArray();
            prices = new JSArray();
            performance = new JSObject();
            event = new JSObject();
            price = new JSObject();
            value = new JSValue();
            string = new ByteSlice();
            eventsKey = asciiKey("events");
            performancesKey = asciiKey("performances");
            eventIdKey = asciiKey("eventId");
            pricesKey = asciiKey("prices");
            amountKey = asciiKey("amount");
            nameKey = asciiKey("name");
        }
    }

    @State(Scope.Thread)
    public static class GemstonesState {
        public byte[] bytes;
        public ByteArrayBuilder buffer;
        public JScreamParser jscreamParser;
        public JSValue root;
        public JSObject gemstones;
        public JSValue value;
        public ByteSlice string;
        public String[] keys;
        public ByteSlice[] keySlices;
        public int expectedCountPerPass;
        public int expectedTotalCount;

        @Setup(Level.Trial)
        public void setUp() {
            bytes = readResource("json/gemstones.json");
            buffer = new ByteArrayBuilder(bytes.length);
            for (byte b : bytes) {
                buffer.appendByte(b);
            }
            jscreamParser = new JScreamParser(JavaDoubleParser::parseDouble);
            root = new JSValue();
            gemstones = new JSObject();
            value = new JSValue();
            string = new ByteSlice();
            keys = new String[] {
                "diamond", "ruby", "sapphire", "emerald", "amethyst",
                "topaz", "opal", "garnet", "aquamarine", "peridot",
                "turquoise", "citrine", "spinel", "zircon", "tourmaline",
                "tanzanite", "moonstone", "sunstone", "lapis", "jade",
                "onyx", "agate", "jasper", "carnelian", "chalcedony",
                "alexandrite", "kunzite", "morganite", "heliodor", "iolite",
                "kyanite", "andalusite", "apatite", "diopside", "tsavorite",
                "rhodolite", "spessartine", "pyrope", "serpentine", "malachite",
                "rose", "lily", "tulip", "daisy", "orchid",
                "violet", "iris", "poppy", "peony", "lotus",
                "jasmine", "lavender", "sunflower", "magnolia", "camellia",
                "gardenia", "hibiscus", "marigold", "azalea", "begonia"
            };
            keySlices = new ByteSlice[keys.length];
            for (int i = 0; i < keys.length; i++) {
                keySlices[i] = asciiKey(keys[i]);
            }
            expectedCountPerPass = 267;
            expectedTotalCount = expectedCountPerPass * 3;
        }
    }

    @Benchmark
    public void parse_jscream(FixtureState state, Blackhole blackhole) {
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
    public void parse_gson(FixtureState state, Blackhole blackhole) {
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
    public void lookup_small_jscream(RfcObjectState state, Blackhole blackhole) {
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
    public void lookup_small_gson(RfcObjectState state, Blackhole blackhole) {
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

    @Benchmark
    public void lookup_large_jscream(CitmCatalogState state, Blackhole blackhole) {
        JSObject catalog = state.jscreamParser.parse(state.buffer, state.root).objectValue(state.catalog);
        JSObject events = catalog.get(state.eventsKey, new JSValue()).objectValue(state.events);
        JSArray performances = catalog.get(state.performancesKey, new JSValue()).arrayValue(state.performances);

        long total = 0L;
        for (int i = 0; i < performances.size(); i++) {
            JSObject performance = performances.valueAt(i, state.value).objectValue(state.performance);
            long eventId = performance.get(state.eventIdKey, state.value).longValue();
            JSValue eventValue = events.get(asciiKey(Long.toString(eventId)), state.value);
            if (eventValue == null) {
                continue;
            }

            JSObject event = eventValue.objectValue(state.event);
            String eventName = event.get(state.nameKey, state.value).stringValue(state.string).toString();
            if (!"Berliner Philharmoniker".equals(eventName)) {
                continue;
            }

            JSArray prices = performance.get(state.pricesKey, state.value).arrayValue(state.prices);
            for (int j = 0; j < prices.size(); j++) {
                JSObject price = prices.valueAt(j, state.value).objectValue(state.price);
                total += price.get(state.amountKey, state.value).longValue();
            }
        }

        blackhole.consume(total);
    }

    @Benchmark
    public void lookup_large_gson(CitmCatalogState state, Blackhole blackhole) {
        JsonObject root = parseGson(state.bytes).getAsJsonObject();
        JsonObject events = root.getAsJsonObject("events");
        JsonArray performances = root.getAsJsonArray("performances");

        long total = 0L;
        for (JsonElement performanceElement : performances) {
            JsonObject performance = performanceElement.getAsJsonObject();
            String eventId = performance.get("eventId").getAsString();
            JsonElement eventElement = events.get(eventId);
            if (eventElement == null) {
                continue;
            }

            JsonObject event = eventElement.getAsJsonObject();
            if (!"Berliner Philharmoniker".equals(event.get("name").getAsString())) {
                continue;
            }

            JsonArray prices = performance.getAsJsonArray("prices");
            for (JsonElement priceElement : prices) {
                total += priceElement.getAsJsonObject().get("amount").getAsLong();
            }
        }

        blackhole.consume(total);
    }

    @Benchmark
    public void lookup_wide_jscream(GemstonesState state, Blackhole blackhole) {
        JSObject gemstones = state.jscreamParser.parse(state.buffer, state.root).objectValue(state.gemstones);

        int count = 0;
        for (int pass = 0; pass < 3; pass++) {
            for (int i = 0; i < state.keys.length; i++) {
                String value = gemstones.get(state.keySlices[i], state.value).stringValue(state.string).toString();
                count += countLowercaseE(value);
            }
        }
        if (count != state.expectedTotalCount) {
            throw new IllegalStateException("unexpected count " + count);
        }

        blackhole.consume(count);
    }

    @Benchmark
    public void lookup_wide_gson(GemstonesState state, Blackhole blackhole) {
        JsonObject gemstones = parseGson(state.bytes).getAsJsonObject();

        int count = 0;
        for (int pass = 0; pass < 3; pass++) {
            for (int i = 0; i < state.keys.length; i++) {
                String value = gemstones.get(state.keys[i]).getAsString();
                count += countLowercaseE(value);
            }
        }
        if (count != state.expectedTotalCount) {
            throw new IllegalStateException("unexpected count " + count);
        }

        blackhole.consume(count);
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

    private static int countLowercaseE(String value) {
        int count = 0;
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) == 'e') {
                count++;
            }
        }
        return count;
    }
}
