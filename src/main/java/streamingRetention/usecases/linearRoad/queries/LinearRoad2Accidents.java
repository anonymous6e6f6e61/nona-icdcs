package streamingRetention.usecases.linearRoad.queries;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import streamingRetention.KafkaOffsetSourceCreator;
import streamingRetention.NonaFlinkSerializerActivator;
import streamingRetention.SecToMillisConverter;
import streamingRetention.ThroughputLoggingMap;
import streamingRetention.usecases.CountTupleGL;
import streamingRetention.usecases.linearRoad.*;
import util.ExperimentSettings;
import util.ProvenanceActivator;
import util.TimestampConverter;

import java.util.Arrays;

import static streamingRetention.usecases.linearRoad.LinearRoadConstants.*;

public class LinearRoad2Accidents {
    public static void main(String[] args) throws Exception {
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final TimestampConverter timestampConverter = new SecToMillisConverter();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableObjectReuse();
        env.setMaxParallelism(1);

        NonaFlinkSerializerActivator.LINEAR_ROAD_FULL.activate(env, settings);

        DataStream<LinearRoadInputTupleGL> sourceStream = env.addSource(KafkaOffsetSourceCreator.create(
                        settings.kafkaSourceTopic(),
                        settings.kafkaSourceBootstrapServer(),
                        new LinearRoadKafkaByteDeserializerGL(),
                        settings.queryID(),
                        settings.sourceLowerBound()))
                .name("LRSource"+settings.queryID())
                .map(new ThroughputLoggingMap<>(settings))
                .name("ThroughputLoggingMap"+settings.queryID())
                .assignTimestampsAndWatermarks(
                    new AscendingTimestampExtractor<LinearRoadInputTupleGL>() {
                        @Override
                        public long extractAscendingTimestamp(LinearRoadInputTupleGL tuple) {
                            return timestampConverter.apply(tuple.getTimestamp());
                        }
                })
                .name("AscendingTimestampExtractor"+settings.queryID())
                .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
                .name("UIDAssigner"+settings.queryID())
                .returns(LinearRoadInputTupleGL.class);

        DataStream<CountTupleGL> accidents1 = sourceStream
                .filter(t -> t.getType() == 0 && t.getSpeed() == 0)
                .name("SpeedFilter1"+settings.queryID())
                .keyBy(t -> t.getKey())
                .window(
                        SlidingEventTimeWindows.of(STOPPED_VEHICLE_WINDOW_SIZE, STOPPED_VEHICLE_WINDOW_SLIDE))
                .aggregate(new VehicleAggregateGL(settings.aggregateStrategySupplier()))
                .name("VehicleAggregateGL1"+settings.queryID())
                .returns(VehicleTupleGL.class)
                .filter(t -> t.getReports() == STOPPED_VEHICLE_MIN_STATIC_POSITIONS && t.isUniquePosition())
                .name("PositionFilter1"+settings.queryID())
                .keyBy(t -> t.getLatestPos())
                .window(SlidingEventTimeWindows.of(ACCIDENT_WINDOW_SIZE, ACCIDENT_WINDOW_SLIDE))
                .aggregate(new AccidentAggregateGL(settings.aggregateStrategySupplier()))
                .name("AccidentAggregateGL1"+settings.queryID())
//        .slotSharingGroup(settings.secondSlotSharingGroup())
                .returns(CountTupleGL.class)
                .filter(t -> t.getCount() > 1)
                .name("AccidentCountFilter1"+settings.queryID());

        DataStream<CountTupleGL> accidents2 = sourceStream
                .filter(t -> t.getType() == 0 && t.getSpeed() == 0)
                .name("SpeedFilter2"+settings.queryID())
                .keyBy(t -> t.getKey())
                .window(
                        SlidingEventTimeWindows.of(STOPPED_VEHICLE_WINDOW_SIZE, STOPPED_VEHICLE_WINDOW_SLIDE))
                .aggregate(new VehicleAggregateGL(settings.aggregateStrategySupplier()))
                .name("VehicleAggregateGL2"+settings.queryID())
                .returns(VehicleTupleGL.class)
                .filter(t -> t.getReports() == STOPPED_VEHICLE_MIN_STATIC_POSITIONS && t.isUniquePosition())
                .name("PositionFilter2"+settings.queryID())
                .keyBy(t -> t.getLatestPos())
                .window(SlidingEventTimeWindows.of(ACCIDENT_WINDOW_SIZE, ACCIDENT_WINDOW_SLIDE))
                .aggregate(new AccidentAggregateGL(settings.aggregateStrategySupplier()))
                .name("AccidentAggregateGL2"+settings.queryID())
//        .slotSharingGroup(settings.secondSlotSharingGroup())
                .returns(CountTupleGL.class)
                .filter(t -> t.getCount() > 1)
                .name("AccidentCountFilter2"+settings.queryID());

        settings
                .genealogActivator()
                .activate(
                        Arrays.asList(
                                ProvenanceActivator.convert(accidents1),
                                ProvenanceActivator.convert(accidents2)
                        ),
                        Arrays.asList("ACCIDENTS1", "ACCIDENTS2"),
                        settings,
                        settings.queryU(),
                        timestampConverter,
                        env.getConfig()
                );

        env.execute("LinearRoad2Accidents queryID" + settings.queryID());
    }
}
