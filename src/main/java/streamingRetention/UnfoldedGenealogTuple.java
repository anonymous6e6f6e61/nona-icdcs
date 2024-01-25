package streamingRetention;

import util.TimestampedTuple;
import util.TimestampedUIDTuple;

import java.io.Serializable;

public interface UnfoldedGenealogTuple extends Serializable, TimestampedTuple {

    static <T1 extends TimestampedUIDTuple,T2 extends TimestampedUIDTuple> UnfoldedGenealogTuple fromString() // return a string representation of the sink and contributing source tuples
    {
        return null;
    }

    Type getType();

    enum Type {
        BPROVENANCE, WATERMARK, MARKER
    }
}
