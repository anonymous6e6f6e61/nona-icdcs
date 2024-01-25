package streamingRetention;

import org.apache.flink.streaming.api.windowing.time.Time;
import util.TimestampConverter;

public class SecToMillisConverter implements TimestampConverter {
    public SecToMillisConverter() {
    }
    @Override
    public Long apply(Long timestamp) {
        return Time.seconds(timestamp).toMilliseconds();
    }
}
