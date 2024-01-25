package streamingRetention;

import util.TimestampedUIDTuple;

public class AnnotatedArrayUnfoldedGenealogTuple extends ArrayUnfoldedGenealogTuple {
    private long threshold;
    private boolean thresholdChanged;

    public AnnotatedArrayUnfoldedGenealogTuple(TimestampedUIDTuple sinkTuple, TimestampedUIDTuple[] sourceTuples,
                                               long threshold, boolean thresholdChanged) {
        super(sinkTuple, sourceTuples);
        this.threshold = threshold;
        this.thresholdChanged = thresholdChanged;
    }

    public static AnnotatedArrayUnfoldedGenealogTuple of(ArrayUnfoldedGenealogTuple tuple) {
        return new AnnotatedArrayUnfoldedGenealogTuple(tuple.getSinkTuple(), tuple.getSourceTuples(), -1, false);
    }

    public static AnnotatedArrayUnfoldedGenealogTuple of(ArrayUnfoldedGenealogTuple tuple, long threshold) {
        return new AnnotatedArrayUnfoldedGenealogTuple(tuple.getSinkTuple(), tuple.getSourceTuples(), threshold, true);
    }

    public long getThreshold() {
        return threshold;
    }

    public boolean isThresholdChanged() {
        return thresholdChanged;
    }
}
