package streamingRetention.playground.dummy;

import genealog.GenealogData;
import genealog.GenealogTuple;
import genealog.GenealogTupleType;

class DummyGLTuple implements GenealogTuple {

    private GenealogData gdata;
    private long timestamp;
    private long stimulus;
    private double value;

    public DummyGLTuple(long timestamp, long stimulus, double value) {
        this.timestamp = timestamp;
        this.stimulus = stimulus;
        this.value = value;
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

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "DummyGLTuple{" +
                "GLTYPE=" + gdata.getTupleType() +
                //"gdata=" + gdata +
                ", timestamp=" + timestamp +
                ", stimulus=" + stimulus +
                ", value=" + value +
                '}';
    }
}

