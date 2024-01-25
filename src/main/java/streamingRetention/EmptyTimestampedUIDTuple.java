package streamingRetention;

import util.TimestampedUIDTuple;

public class EmptyTimestampedUIDTuple implements TimestampedUIDTuple {
    @Override
    public long getTimestamp() {
        return 0;
    }

    @Override
    public void setTimestamp(long timestamp) {
    }

    @Override
    public long getStimulus() {
        return 0;
    }

    @Override
    public void setStimulus(long stimulus) {
    }

    @Override
    public long getUID() {
        return 0;
    }

    @Override
    public void setUID(long uid) {
    }
}
