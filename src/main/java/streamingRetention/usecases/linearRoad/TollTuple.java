package streamingRetention.usecases.linearRoad;

import util.BaseTuple;

public class TollTuple extends BaseTuple {

    public TollTuple(long timestamp, String key, long stimulus) {
        super(timestamp, key, stimulus);
    }

    @Override
    public String toString() {
        return getTimestamp() + "," + key;
    }

    public static TollTuple fromString(String s) {
        String[] ss = s.split(",");
        return new TollTuple(Long.parseLong(ss[0]), ss[1], -1L);
    }
}
