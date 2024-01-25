package streamingRetention;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import util.AvgStat;
import util.ExperimentSettings;
import util.TimestampedTuple;

public class LatencyLogMap<T extends TimestampedTuple> extends RichMapFunction<T,T> {

    private final ExperimentSettings settings;
    private transient AvgStat latencyStatistic;

    public LatencyLogMap(ExperimentSettings settings) {
        this.settings = settings;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.latencyStatistic = new AvgStat(settings.latencyFile(0, settings.queryID()), settings.autoFlush());
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        latencyStatistic.close();
        super.close();
    }

    @Override
    public T map(T t) throws Exception {
        latencyStatistic.add(System.currentTimeMillis() - t.getStimulus());
        return t;
    }
}
