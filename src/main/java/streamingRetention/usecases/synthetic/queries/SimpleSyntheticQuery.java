package streamingRetention.usecases.synthetic.queries;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import streamingRetention.*;
import streamingRetention.usecases.synthetic.SyntheticProvenanceSource;
import streamingRetention.usecases.synthetic.SyntheticSinkTuple;
import streamingRetention.usecases.synthetic.SyntheticSourceTuple;
import util.ExperimentSettings;
import util.ProvenanceActivator;
import util.TimestampConverter;
import java.util.Arrays;


public class SimpleSyntheticQuery {

    public static void main(String[] args) throws Exception {
        ExperimentSettings settings = ExperimentSettings.newInstance(args);

        final TimestampConverter timestampConverter = new SecToMillisConverter();

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableObjectReuse();
        env.setMaxParallelism(1);

        NonaFlinkSerializerActivator.SYNTHETIC.activate(env, settings);

        DataStream<SyntheticSinkTuple> syntheticSourceStream = env.addSource(new SyntheticProvenanceSource<>(
                        SyntheticSinkTuple.supplier(),
                        SyntheticSourceTuple.supplier(settings.synthetic_tupleSize()),
                        settings)
                )
                .name("SyntheticSource"+settings.queryID())
                .returns(SyntheticSinkTuple.class)
                .map(new ThroughputLoggingMap<>(settings))
                .name("ThroughputLoggingMap"+settings.queryID())
                .returns(SyntheticSinkTuple.class)
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<SyntheticSinkTuple>() {
                            @Override
                            public long extractAscendingTimestamp(SyntheticSinkTuple tuple) {
                                return timestampConverter.apply(tuple.getTimestamp());
                            }
                        })
                .name("AscendingTimestampExtractor"+settings.queryID())
                .returns(SyntheticSinkTuple.class);

        settings
                .genealogActivator()
                .activate(
                        Arrays.asList(ProvenanceActivator.convert(syntheticSourceStream)),
                        Arrays.asList(settings.queryID()),
                        settings,
                        settings.queryU(),
                        timestampConverter,
                        env.getConfig()
                );

        env.execute("Synthetic queryID" + settings.queryID());
    }
}
