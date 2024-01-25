package streamingRetention.usecases.carLocal.queries;

import java.util.Arrays;
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

import static streamingRetention.usecases.carLocal.CarLocalConstants.*;

public class CarLocalCyclists {

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

        DataStream<CyclesInFrontTuple> cyclesInFront =
                LidarLeftRightTimestamped
                        .flatMap(new CarLocalCyclistsToSingleObjectGL())
                        .name("FlatMap"+settings.queryID())
                        .filter(new CarLocalFilterBicyclesGL())
                        .name("BikeFilter"+settings.queryID())
                        .keyBy(t -> t.f0)
                        .window(SlidingEventTimeWindows
                                .of(CYCLIST_COUNTING_WINDOW_SIZE, CYCLIST_COUNTING_WINDOW_ADVANCE))
                        .aggregate(new CarLocalCountGL(settings.aggregateStrategySupplier()))
                        .name("CarLocalCountGL"+settings.queryID())
                        .filter(new CarLocalFilterCyclesInFrontGL())
                        .name("CarLocalFilterCyclesInFrontGL"+settings.queryID());

        settings
                .genealogActivator()
                .activate(
                        Arrays.asList(
                                ProvenanceActivator.convert(cyclesInFront)),
                        Arrays.asList(settings.queryID()),
                        settings,
                        settings.queryU(),
                        timestampConverter,
                        env.getConfig());

        env.execute("CarLocalCyclists queryID" + settings.queryID());
    }

}

