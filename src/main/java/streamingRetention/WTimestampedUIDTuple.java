package streamingRetention;

import util.TimestampedUIDTuple;

public class WTimestampedUIDTuple implements TimestampedUIDTuple {

    /**
     * Implementation of {@link TimestampedUIDTuple} that represents a
     * {@link org.apache.flink.streaming.api.watermark.Watermark}
     */

    private long timestampFromWatermark;
    private long stimulus;
    private long uid;

    public WTimestampedUIDTuple(long timestampFromWatermark, long stimulus, long uid) {
        this.timestampFromWatermark = timestampFromWatermark;
        this.stimulus = stimulus;
        this.uid = uid;
    }

    public static WTimestampedUIDTuple of(long timestampFromWatermark) {
        return new WTimestampedUIDTuple(timestampFromWatermark, -1, 0);
    }

    @Override
    public long getTimestamp() {
        return timestampFromWatermark;
    }

    @Override
    public void setTimestamp(long timestamp) {
        this.timestampFromWatermark = timestamp;
    }

    @Override
    public long getStimulus() {
        return stimulus;
    }

    @Override
    public void setStimulus(long stimulus) {
        this.stimulus = stimulus;
    }

    @Override
    public long getUID() {
        return uid;
    }

    @Override
    public void setUID(long uid) {
        this.uid = uid;
    }

    @Override
    public String toString() {
        return Long.toString(timestampFromWatermark);
    }
}
