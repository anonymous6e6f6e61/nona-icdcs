package streamingRetention;

import util.TimestampedUIDTuple;
import java.util.function.Function;

public class ArrayUnfoldedGenealogTuple implements UnfoldedGenealogTuple {

    private long timestamp;
    private long stimulus;
    private final TimestampedUIDTuple[] sourceTuples;
    private final TimestampedUIDTuple sinkTuple;
    private final Type type;

    public ArrayUnfoldedGenealogTuple(TimestampedUIDTuple sinkTuple, TimestampedUIDTuple[] sourceTuples) {
        this(sinkTuple.getTimestamp(), sinkTuple.getStimulus(), sourceTuples, sinkTuple, Type.BPROVENANCE);
    }

    public ArrayUnfoldedGenealogTuple(WTimestampedUIDTuple watermarkTuple) {
        this(watermarkTuple.getTimestamp(), watermarkTuple.getStimulus(), new TimestampedUIDTuple[]{}, watermarkTuple, Type.WATERMARK);
    }

    public static ArrayUnfoldedGenealogTuple getMarker() {
        return new ArrayUnfoldedGenealogTuple(0L, 0L, new TimestampedUIDTuple[]{}, new EmptyTimestampedUIDTuple(),
                Type.MARKER);
    }

    public ArrayUnfoldedGenealogTuple(long timestamp, long stimulus, TimestampedUIDTuple[] sourceTuples,
                                      TimestampedUIDTuple sinkTuple, Type type) {
        this.timestamp = timestamp;
        this.stimulus = stimulus;
        this.sourceTuples = sourceTuples;
        this.sinkTuple = sinkTuple;
        this.type = type;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
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
    public String toString() {
        StringBuilder returnStr = new StringBuilder();
        returnStr.append(sinkTuple)
                .append(";")
                .append(sinkTuple.getTimestamp())
                .append(";")
                .append(sinkTuple.getStimulus())
                .append(";")
                .append(sinkTuple.getUID());
        for (int i=1; i< sourceTuples.length; i++) {
            returnStr.append("/")
                    .append(sourceTuples[i])
                    .append(";")
                    .append(sourceTuples[i].getTimestamp())
                    .append(";")
                    .append(sourceTuples[i].getStimulus())
                    .append(";")
                    .append(sourceTuples[i].getUID());
        }
        return returnStr.toString();
    }


    public static <T1 extends TimestampedUIDTuple,T2 extends TimestampedUIDTuple> UnfoldedGenealogTuple fromString(String s,
                                                 Function<String, T1> sinkTupleFromString,
                                                 Function<String, T2> sourceTupleFromString) {
        String[] splitInputString = s.split("/");

        String sinkString = splitInputString[0];
        TimestampedUIDTuple sinkTuple = sinkTupleFromString.apply(sinkString.split(";")[0]);
        sinkTuple.setTimestamp(Long.parseLong(sinkString.split(";")[1]));
        sinkTuple.setStimulus(Long.parseLong(sinkString.split(";")[2]));
        sinkTuple.setUID(Long.parseLong(sinkString.split(";")[3]));

        TimestampedUIDTuple[] sourceTuples = new TimestampedUIDTuple[splitInputString.length-1];
        for (int i=1; i< splitInputString.length; i++) {
            String sourceString = splitInputString[i];
            sourceTuples[i-1] = sourceTupleFromString.apply(sourceString.split(";")[0]);
            sourceTuples[i-1].setTimestamp(Long.parseLong(sourceString.split(";")[1]));
            sourceTuples[i-1].setStimulus(Long.parseLong(sourceString.split(";")[2]));
            sourceTuples[i-1].setUID(Long.parseLong(sourceString.split(";")[3]));
        }
        return new ArrayUnfoldedGenealogTuple(sinkTuple, sourceTuples);
    }

    @Override
    public Type getType() {
        return this.type;
    }

    public TimestampedUIDTuple[] getSourceTuples() {
        return sourceTuples;
    }

    public TimestampedUIDTuple getSinkTuple() {
        return sinkTuple;
    }


}
