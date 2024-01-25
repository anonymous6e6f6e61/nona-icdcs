package streamingRetention.usecases.carLocal.queries;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import streamingRetention.KafkaOffsetSourceCreator;
import streamingRetention.NonaFlinkSerializerActivator;
import streamingRetention.ThroughputLoggingMap;
import streamingRetention.usecases.carLocal.*;
import util.ExperimentSettings;
import util.ProvenanceActivator;
import util.TimestampConverter;

import java.util.Arrays;

import static streamingRetention.usecases.carLocal.CarLocalConstants.*;

public class CarLocal2Cyclists {

    public static void main(String[] args) throws Exception {

        ExperimentSettings settings = ExperimentSettings.newInstance(args);

        final TimestampConverter timestampConverter = ts -> ts;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setMaxParallelism(settings.maxParallelism());
        env.getConfig().enableObjectReuse();

        NonaFlinkSerializerActivator.CAR_LOCAL_FULL.activate(env, settings);

        // connect to source
        SingleOutputStreamOperator<CarLocalInputTupleGL>
                LidarLeftRightTimestamped = env.addSource(KafkaOffsetSourceCreator.create(
                    settings.kafkaSourceTopic(),
                    settings.kafkaSourceBootstrapServer(),
                    new CarLocalByteArrayDeserializerGL(),
                    settings.queryID(),
                    settings.sourceLowerBound()))
                .name("CLSource"+settings.queryID())
                .map(new ThroughputLoggingMap<>(settings))
                .name("ThroughputLoggingMap"+settings.queryID())
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<CarLocalInputTupleGL>() {
                            @Override
                            public long extractAscendingTimestamp(CarLocalInputTupleGL tuple) {
                                return timestampConverter.apply(tuple.getTimestamp());
                            }
                        })
                .name("AscendingTimestampExtractor"+settings.queryID())
                .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
                .name("UIDAssigner"+settings.queryID())
                .returns(CarLocalInputTupleGL.class);

        // ----------------------------------------------------------------------------
        // ----------------------------- QUERY 1 --------------------------------------
        // ----------------------------------------------------------------------------

        DataStream<CyclesInFrontTuple> cyclesInFront1 =
                LidarLeftRightTimestamped
                        .flatMap(new CarLocalCyclistsToSingleObjectGL())
                        .name("FlatMap1"+settings.queryID())
                        .filter(new CarLocalFilterBicyclesGL())
                        .name("BikeFilter1"+settings.queryID())
                        .keyBy(t -> t.f0)
                        .window(SlidingEventTimeWindows
                                .of(CYCLIST_COUNTING_WINDOW_SIZE, CYCLIST_COUNTING_WINDOW_ADVANCE))
                        .aggregate(new CarLocalCountGL(settings.aggregateStrategySupplier()))
                        .name("CarLocalCountGL1"+settings.queryID())
                        .filter(new CarLocalFilterCyclesInFrontGL())
                        .name("CarLocalFilterCyclesInFrontGL1"+settings.queryID());

        // ----------------------------------------------------------------------------
        // ----------------------------- QUERY 2 --------------------------------------
        // ----------------------------------------------------------------------------

        DataStream<CyclesInFrontTuple> cyclesInFront2 =
                LidarLeftRightTimestamped
                        .flatMap(new CarLocalCyclistsToSingleObjectGL())
                        .name("FlatMap2"+settings.queryID())
                        .filter(new CarLocalFilterBicyclesGL())
                        .name("BikeFilter2"+settings.queryID())
                        .keyBy(t -> t.f0)
                        .window(SlidingEventTimeWindows
                                .of(CYCLIST_COUNTING_WINDOW_SIZE, CYCLIST_COUNTING_WINDOW_ADVANCE))
                        .aggregate(new CarLocalCountGL(settings.aggregateStrategySupplier()))
                        .name("CarLocalCountGL2"+settings.queryID())
                        .filter(new CarLocalFilterCyclesInFrontGL())
                        .name("CarLocalFilterCyclesInFrontGL2"+settings.queryID());

        settings
                .genealogActivator()
                .activate(
                        Arrays.asList(
                                ProvenanceActivator.convert(cyclesInFront1),
                                ProvenanceActivator.convert(cyclesInFront2)),
                        Arrays.asList("CYCLES1", "CYCLES2"),
                        settings,
                        settings.queryU(),
                        timestampConverter,
                        env.getConfig());

        env.execute("CarLocal2Cyclists queryID" + settings.queryID());
    }

}

