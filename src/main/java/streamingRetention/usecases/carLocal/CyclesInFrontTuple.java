package streamingRetention.usecases.carLocal;

import genealog.GenealogData;
import genealog.GenealogTuple;
import genealog.GenealogTupleType;
import org.apache.flink.api.java.tuple.Tuple3;

public class CyclesInFrontTuple extends Tuple3<String, Integer, Long> implements GenealogTuple {

  //Fields need to be public for Flink to serialize correctly
  public long stimulus;
  public long timestamp;
  public GenealogData genealogData;

  public CyclesInFrontTuple() {
  }

  public CyclesInFrontTuple(String value0, int value1, long value2) {
    super(value0, value1, value2);
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
