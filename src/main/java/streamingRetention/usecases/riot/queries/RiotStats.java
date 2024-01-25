package streamingRetention.usecases.riot.queries;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import streamingRetention.*;
import streamingRetention.usecases.carLocal.Tuple2GL;
import streamingRetention.usecases.carLocal.Tuple3GL;
import streamingRetention.usecases.riot.*;
import util.ExperimentSettings;
import util.ProvenanceActivator;
import util.TimestampConverter;
import java.util.Arrays;

import static streamingRetention.usecases.riot.StatsConstants.*;

public class RiotStats {

    public static void main(String[] args) throws Exception {
        ExperimentSettings settings = ExperimentSettings.newInstance(args);

        final TimestampConverter timestampConverter = new SecToMillisConverter();

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableObjectReuse();
        env.setMaxParallelism(1);

        NonaFlinkSerializerActivator.LINEAR_ROAD_FULL.activate(env, settings);

        DataStream<MhealthInputTupleGL> sourceStream =  env.addSource(KafkaOffsetSourceCreator.create(
                settings.kafkaSourceTopic(),
                settings.kafkaSourceBootstrapServer(),
                new MhealthStringDeserializerGL(),
                settings.queryID(),
                settings.sourceLowerBound()))
            .name("RISource" + settings.queryID())
            .returns(MhealthInputTupleGL.class)
            .map(new ThroughputLoggingMap<>(settings))
            .name("ThroughputLoggingMap" + settings.queryID())
            .assignTimestampsAndWatermarks(
                    new AscendingTimestampExtractor<MhealthInputTupleGL>() {
                        @Override
                        public long extractAscendingTimestamp(MhealthInputTupleGL tuple) {
                            return timestampConverter.apply(tuple.getTimestamp());
                        }
                    }
            )
            .name("AscendingTimestampExtractor"+settings.queryID())
            .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
            .name("UIDAssigner"+settings.queryID())
            .returns(MhealthInputTupleGL.class);

        DataStream<Tuple2GL<Double, String>> ecg1stream = sourceStream
                .keyBy(MhealthInputTupleGL::getKey)
                .window(SlidingEventTimeWindows.of(ECG1_AVERAGE_WINDOW_SIZE, ECG1_AVERAGE_WINDOW_ADVANCE))
                .aggregate(new StatsEcg1Average(settings.aggregateStrategySupplier()))
                .name("StatsEcg1Average" + settings.queryID())
                .filter(t -> t.f0 > 2.0)
                .name("ECG2Filter" + settings.queryID());

        DataStream<Tuple2GL<Double, String>> ecg2stream = sourceStream
                .keyBy(MhealthInputTupleGL::getKey)
                .window(SlidingEventTimeWindows.of(ECG2_AVERAGE_WINDOW_SIZE, ECG2_AVERAGE_WINDOW_ADVANCE))
                .aggregate(new StatsEcg2Average(settings.aggregateStrategySupplier()))
                .name("StatsEcg2Average" + settings.queryID())
                .filter(t -> t.f0 > 2.0)
                .name("ECG2Filter" + settings.queryID());

        DataStream<Tuple7GL<Double, Double, Double, Double, Double, Double, String>>
                motionPredictorStream = sourceStream
                .keyBy(MhealthInputTupleGL::getKey)
                .window(SlidingEventTimeWindows.of(MOTION_WINDOW_SIZE, MOTION_WINDOW_ADVANCE))
                .aggregate(new StatsMotionLinearRegression(settings.aggregateStrategySupplier()))
                .name("StatsMotion" + settings.queryID());

        DataStream<Tuple3GL<Double, Double, String>> joinedEcgStream = ecg1stream
                .join(ecg2stream)
                .where( (KeySelector<Tuple2GL<Double, String>, String>) left -> left.f1 )
                .equalTo( (KeySelector<Tuple2GL<Double, String>, String>) right -> right.f1)
                .window(SlidingEventTimeWindows.of(ECG_JOIN_WINDOW_SIZE, ECG_JOIN_WINDOW_ADVANCE))
                .apply(new StatsEcgJoin());

        DataStream<Tuple1GL<String>> resultStream = joinedEcgStream
                .join(motionPredictorStream)
                .where( (KeySelector<Tuple3GL<Double, Double, String>, String>) left -> left.f2 )
                .equalTo(
                        (KeySelector<Tuple7GL<Double, Double, Double, Double, Double, Double, String>, String>) right
                                -> right.f6)
                .window(SlidingEventTimeWindows.of(FINAL_JOIN_WINDOW_SIZE, FINAL_JOIN_WINDOW_ADVANCE))
                .apply(new StatsFinalJoin());

        settings
                .genealogActivator()
                .activate(
                        Arrays.asList(ProvenanceActivator.convert(resultStream)),
                        Arrays.asList(settings.queryID()),
                        settings,
                        settings.queryU(),
                        timestampConverter,
                        env.getConfig()
                );

        env.execute("RiotStats queryID" + settings.queryID());
    }
}

