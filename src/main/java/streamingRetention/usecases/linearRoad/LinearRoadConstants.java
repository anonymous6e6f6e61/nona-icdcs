package streamingRetention.usecases.linearRoad;

import org.apache.flink.streaming.api.windowing.time.Time;

public class LinearRoadConstants {

  public static final Time STOPPED_VEHICLE_WINDOW_SIZE = Time.seconds(120);
  public static final Time STOPPED_VEHICLE_WINDOW_SLIDE = Time.seconds(30);
  public static final int STOPPED_VEHICLE_MIN_STATIC_POSITIONS = 4;

  public static final Time ACCIDENT_WINDOW_SIZE = Time.seconds(30);
  public static final Time ACCIDENT_WINDOW_SLIDE = Time.seconds(30);

  public static final Time TOLL_NUMVEHICLES_WINDOW_SIZE = Time.seconds(60);
  public static final Time TOLL_NUMVEHICLES_WINDOW_SLIDE = Time.seconds(60);
  public static final Time TOLL_AVGSV_WINDOW_SIZE = Time.seconds(60);
  public static final Time TOLL_AVGSV_WINDOW_SLIDE = Time.seconds(60);
  public static final Time TOLL_AVGS_WINDOW_SIZE = Time.seconds(60);
  public static final Time TOLL_AVGS_WINDOW_SLIDE = Time.seconds(60);
  public static final Time TOLL_LAV_WINDOW_SIZE = Time.seconds(300);
  public static final Time TOLL_LAV_WINDOW_SLIDE = Time.seconds(60);
  public static final int TOLL_NUMVEHICLES_MIN = 50;
  public static final int TOLL_LAV_MAX = 40;

  private LinearRoadConstants() {

  }

}
