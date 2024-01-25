package streamingRetention;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.function.BiFunction;
import java.util.function.Function;

public class SimpleNamedWatermarkConversionFunction
        implements BiFunction<Watermark, String, StreamRecord<UnfoldedGenealogTuple>>, Serializable {

    private final Function<String,Long> nameUIDFunction;

    public SimpleNamedWatermarkConversionFunction(Function<String, Long> nameUIDFunction) {
        this.nameUIDFunction = nameUIDFunction;
    }

    public StreamRecord<UnfoldedGenealogTuple> apply(Watermark mark, String name) {
        WTimestampedUIDTuple fromWatermark = WTimestampedUIDTuple.of(mark.getTimestamp());
        fromWatermark.setUID(nameUIDFunction.apply(name));
        ArrayUnfoldedGenealogTuple aFromWatermark = new ArrayUnfoldedGenealogTuple(fromWatermark);
        return new StreamRecord<>(aFromWatermark, mark.getTimestamp());
    }

}
