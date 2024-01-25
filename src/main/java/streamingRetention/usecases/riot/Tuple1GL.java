package streamingRetention.usecases.riot;

import genealog.GenealogData;
import genealog.GenealogTuple;
import genealog.GenealogTupleType;
import java.io.Serializable;

public class Tuple1GL<T> implements GenealogTuple, Serializable {

  //Fields need to be public for Flink to serialize correctly
  public long stimulus;
  public long timestamp;
  public GenealogData genealogData;
  public T f0;

  public Tuple1GL() {
  }

  public Tuple1GL(T value0) {
    f0 = value0;
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
