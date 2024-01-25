package streamingRetention.usecases.carLocal;

import genealog.GenealogData;
import genealog.GenealogTuple;
import genealog.GenealogTupleType;
import org.apache.flink.api.java.tuple.Tuple2;

public class CrossedPedestriansTuple extends Tuple2<String, Long> implements GenealogTuple {

  //Fields need to be public for Flink to serialize correctly
  public long stimulus;
  public long timestamp;
  public GenealogData genealogData;

  public CrossedPedestriansTuple() {
  }

  public CrossedPedestriansTuple(String value0, long value1) {
    super(value0, value1);
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
