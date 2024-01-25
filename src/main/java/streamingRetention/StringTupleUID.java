package streamingRetention;
import util.TimestampedUIDTuple;

public class StringTupleUID implements TimestampedUIDTuple {

    private String value;
    private long timestamp;
    private long stimulus;
    private long uid;

    public StringTupleUID(long timestamp, String s) {
        this.timestamp= timestamp;
        this.value = s;
    }

    public String getS() {
        return value;
    }

    public void setS(String s) {
        this.value = s;
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
    public long getUID() {
        return uid;
    }

    @Override
    public void setUID(long uid) {
        this.uid = uid;
    }

    @Override
    public String toString() {
        return getTimestamp() + "," + value;
    }

    public static StringTupleUID fromString(String s) {
        return new StringTupleUID(Long.parseLong(s.split(",")[0]), s);
    }
}
