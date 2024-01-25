package streamingRetention.usecases.riot;

import org.apache.flink.streaming.api.windowing.time.Time;

public class StatsConstants {
    public static final Time ECG1_AVERAGE_WINDOW_SIZE = Time.seconds(5);
    public static final Time ECG1_AVERAGE_WINDOW_ADVANCE = Time.seconds(5);

    public static final Time ECG2_AVERAGE_WINDOW_SIZE = Time.seconds(5);
    public static final Time ECG2_AVERAGE_WINDOW_ADVANCE = Time.seconds(5);

    public static final Time MOTION_WINDOW_SIZE = Time.seconds(1);
    public static final Time MOTION_WINDOW_ADVANCE = Time.seconds(1);

    public static final Time ECG_JOIN_WINDOW_SIZE = Time.seconds(5);
    public static final Time ECG_JOIN_WINDOW_ADVANCE = Time.seconds(5);

    public static final Time FINAL_JOIN_WINDOW_SIZE = Time.seconds(5);
    public static final Time FINAL_JOIN_WINDOW_ADVANCE = Time.seconds(5);
}
