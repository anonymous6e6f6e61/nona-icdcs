package streamingRetention.usecases.linearRoad;

import genealog.GenealogData;
import genealog.GenealogTuple;
import genealog.GenealogTupleType;

public class TollTupleGL extends TollTuple implements GenealogTuple {

    private GenealogData gdata;

    public TollTupleGL(long timestamp, String key, long stimulus) {
        super(timestamp, key, stimulus);
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
}
