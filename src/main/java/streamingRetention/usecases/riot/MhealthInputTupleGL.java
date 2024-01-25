package streamingRetention.usecases.riot;

import genealog.GenealogData;
import genealog.GenealogTuple;
import genealog.GenealogTupleType;

import java.util.regex.Pattern;

public class MhealthInputTupleGL extends MhealthInputTuple implements GenealogTuple {

    private static final Pattern DELIMITER_PATTERN = Pattern.compile(",");
    private GenealogData gdata;

    protected MhealthInputTupleGL(String[] readings) {
        super(readings);
    }

    protected MhealthInputTupleGL(long timestamp, long stimulus, double accelerationChestX, double accelerationChestY,
                               double accelerationChestZ, double ecgSignalLead1, double ecgSignalLead2,
                               int activityLabel) {
        super(timestamp, stimulus, accelerationChestX, accelerationChestY, accelerationChestZ, ecgSignalLead1,
                ecgSignalLead2, activityLabel);
    }

    public static MhealthInputTupleGL fromReading(String reading) {
        try {
            String[] tokens = DELIMITER_PATTERN.split(reading.trim());
            MhealthInputTupleGL tuple = new MhealthInputTupleGL(tokens);
            tuple.initGenealog(GenealogTupleType.SOURCE);
            return tuple;
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                    "Failed to parse reading: %s", reading), e);
        }
    }

    @Override
    public GenealogTuple getU1() {
        return gdata.getU1();
    }

    @Override
    public void setU1(GenealogTuple u1) {
        gdata.setU1(u1);
    }

    @Override
    public GenealogTuple getU2() {
        return gdata.getU2();
    }

    @Override
    public void setU2(GenealogTuple u2) {
        gdata.setU2(u2);
    }

    @Override
    public GenealogTuple getNext() {
        return gdata.getNext();
    }

    @Override
    public void setNext(GenealogTuple next) {
        gdata.setNext(next);
    }

    @Override
    public GenealogTupleType getTupleType() {
        return gdata.getTupleType();
    }

    @Override
    public void initGenealog(GenealogTupleType tupleType) {
        gdata = new GenealogData();
        gdata.init(tupleType);
    }

    @Override
    public long getUID() {
        return gdata.getUID();
    }

    @Override
    public void setUID(long uid) {
        gdata.setUID(uid);
    }

    @Override
    public GenealogData getGenealogData() {
        return gdata;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
