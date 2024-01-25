package streamingRetention.usecases.riot;

import genealog.GenealogData;
import genealog.GenealogTuple;
import genealog.GenealogTupleType;
import org.apache.flink.api.java.tuple.Tuple7;


public class Tuple7GL<T1, T2, T3, T4, T5, T6, T7> extends Tuple7<T1, T2, T3, T4, T5, T6, T7> implements GenealogTuple {
    public long stimulus;
    public long timestamp;
    public GenealogData genealogData;

    public Tuple7GL() {
    }

    public Tuple7GL(T1 value0, T2 value1, T3 value2, T4 value3, T5 value4, T6 value5, T7 value6) {
        super(value0, value1, value2, value3, value4, value5, value6);
    }

    @Override
    public void initGenealog(GenealogTupleType tupleType) {
        this.genealogData = new GenealogData();
        this.genealogData.init(tupleType);
    }

    @Override
    public GenealogData getGenealogData() {
        return genealogData;
    }

    public void setGenealogData(GenealogData genealogData) {
        this.genealogData = genealogData;
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
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
