package streamingRetention.usecases.linearRoad;

import genealog.GenealogData;
import genealog.GenealogTuple;
import genealog.GenealogTupleType;

import java.util.regex.Pattern;

public class LinearRoadInputTupleGL extends
    LinearRoadInputTuple implements GenealogTuple {

  private static final Pattern DELIMITER_PATTERN = Pattern.compile(",");
  private GenealogData gdata;

  protected LinearRoadInputTupleGL(String[] readings) {
    super(readings);
  }

  protected LinearRoadInputTupleGL(int type, long time, int vid, int speed, int xway, int lane,
      int dir, int seg, int pos, long stimulus) {
    super(type, time, vid, speed, xway, lane, dir, seg, pos, stimulus);
  }

  public static LinearRoadInputTupleGL fromReading(String reading) {
    try {
      String[] tokens = DELIMITER_PATTERN.split(reading.trim());
      LinearRoadInputTupleGL tuple = new LinearRoadInputTupleGL(tokens);
      tuple.initGenealog(GenealogTupleType.SOURCE);
      return tuple;
    }
    catch (Exception exception) {
      throw new IllegalArgumentException(String.format(
          "Failed to parse reading: %s", reading), exception);
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

  public static LinearRoadInputTupleGL fromString(String s) {
    String[] ss = s.split(",");
    LinearRoadInputTupleGL lr = new LinearRoadInputTupleGL(
            Integer.parseInt(ss[0]),
            Long.parseLong(ss[1]),
            Integer.parseInt(ss[2]),
            Integer.parseInt(ss[3]),
            Integer.parseInt(ss[4]),
            Integer.parseInt(ss[5]),
            Integer.parseInt(ss[6]),
            Integer.parseInt(ss[7]),
            Integer.parseInt(ss[8]),
            -1L);
    lr.initGenealog(GenealogTupleType.SOURCE);
    return lr;
  }
}
