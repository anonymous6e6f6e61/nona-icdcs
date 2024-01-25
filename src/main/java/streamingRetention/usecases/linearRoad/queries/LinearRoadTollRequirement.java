package streamingRetention.usecases.linearRoad.queries;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import streamingRetention.*;
import streamingRetention.usecases.CountTupleGL;
import streamingRetention.usecases.linearRoad.*;
import util.ExperimentSettings;
import util.ProvenanceActivator;
import util.TimestampConverter;
import java.util.Arrays;
import static streamingRetention.usecases.linearRoad.LinearRoadConstants.*;

public class LinearRoadTollRequirement {

    public static void main(String[] args) throws Exception {
        ExperimentSettings settings = ExperimentSettings.newInstance(args);

        final TimestampConverter timestampConverter = new SecToMillisConverter();

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableObjectReuse();
        env.setMaxParallelism(1);

        NonaFlinkSerializerActivator.LINEAR_ROAD_FULL.activate(env, settings);

        DataStream<LinearRoadInputTupleGL> vehicleSourceStream = env.addSource(KafkaOffsetSourceCreator.create(
                        settings.kafkaSourceTopic(),
                        settings.kafkaSourceBootstrapServer(),
                        new LinearRoadKafkaByteDeserializerGL(),
                        settings.queryID(),
                        settings.sourceLowerBound()))
                .name("LinearRoadSource"+settings.queryID())
                .map(new ThroughputLoggingMap<>(settings))
                .assignTimestampsAndWatermarks(
                    new AscendingTimestampExtractor<LinearRoadInputTupleGL>() {
                        @Override
                        public long extractAscendingTimestamp(LinearRoadInputTupleGL tuple) {
                            return timestampConverter.apply(tuple.getTimestamp());
                        }
                })
                .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
                .returns(LinearRoadInputTupleGL.class)
                .filter(t -> t.getType() == 0);

        DataStream<CountTupleGL> numVehiclesStream =
                vehicleSourceStream
                .keyBy( t -> MultiKeyLR.of(t.getSeg(), t.getDir()))
                .window(SlidingEventTimeWindows.of(TOLL_NUMVEHICLES_WINDOW_SIZE, TOLL_NUMVEHICLES_WINDOW_SLIDE))
                .aggregate(new UniqueVehiclesAggregateGL(settings.aggregateStrategySupplier()))
                .returns(CountTupleGL.class)
                .filter(t -> t.getCount() >= TOLL_NUMVEHICLES_MIN);

        DataStream<LavTupleGL> lavStream =
                vehicleSourceStream
                .keyBy(t -> MultiKeyLR.of(t.getSeg(), t.getDir(), t.getVid()))
                .window(SlidingEventTimeWindows.of(TOLL_AVGSV_WINDOW_SIZE, TOLL_AVGSV_WINDOW_SLIDE))
                .aggregate(new AVGVSaggregateGL(settings.aggregateStrategySupplier()))
                .returns(LavTupleGL.class)
                .keyBy(t -> MultiKeyLR.segDir(t))
                .window(SlidingEventTimeWindows.of(TOLL_AVGS_WINDOW_SIZE, TOLL_AVGS_WINDOW_SLIDE))
                .aggregate(new AVGVaggregateGL(settings.aggregateStrategySupplier()))
                .returns(LavTupleGL.class)
                .keyBy(t -> MultiKeyLR.segDir(t))
                .window(SlidingEventTimeWindows.of(TOLL_LAV_WINDOW_SIZE, TOLL_LAV_WINDOW_SLIDE))
                .aggregate(new LAVaggregateGL(settings.aggregateStrategySupplier()))
                .returns(LavTupleGL.class)
                .filter(t -> t.getLAV() <= TOLL_LAV_MAX);

           DataStream<TollTupleGL> tollStream = numVehiclesStream
                .join(lavStream)
                .where(t -> t.getKey())
                .equalTo(t -> t.getKey())
                .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(60)))
                .apply(new TollJoinGL());

        settings
                .genealogActivator()
                .activate(
                        Arrays.asList(ProvenanceActivator.convert(tollStream)),
                        Arrays.asList(settings.queryID()),
                        settings,
                        settings.queryU(),
                        timestampConverter,
                        env.getConfig()
                );

        env.execute("LinearRoadTollRequirement queryID" + settings.queryID());


    }
}
