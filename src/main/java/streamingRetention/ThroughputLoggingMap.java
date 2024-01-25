package streamingRetention;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import util.CountStat;
import util.ExperimentSettings;

public class ThroughputLoggingMap<T> extends RichMapFunction<T, T> {

    private transient CountStat throughputStat;
    private final ExperimentSettings settings;
    private int additionalIdentifier;

    public ThroughputLoggingMap(ExperimentSettings settings) {
        this.settings = settings;
    }

    public ThroughputLoggingMap(ExperimentSettings settings, int additional_identifier) {
        this.settings = settings;
        this.additionalIdentifier = additional_identifier;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.throughputStat =
                new CountStat(
                        settings.throughputFile(settings.queryID(), additionalIdentifier),
                        settings.autoFlush());
    }

    @Override
    public T map(T t) throws Exception {
        throughputStat.increase(1);
        return t;
    }

    @Override
    public void close() throws Exception {
        super.close();
        throughputStat.close();
    }
}