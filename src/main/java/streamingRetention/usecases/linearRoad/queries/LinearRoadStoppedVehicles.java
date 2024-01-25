package streamingRetention.usecases.linearRoad.queries;

import org.apache.flink.streaming.api.datastream.DataStream;
import streamingRetention.*;
import streamingRetention.usecases.linearRoad.*;
import util.ExperimentSettings;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import util.ProvenanceActivator;
import util.TimestampConverter;

import java.util.Arrays;

import static streamingRetention.usecases.linearRoad.LinearRoadConstants.*;

public class LinearRoadStoppedVehicles {

  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);
    final TimestampConverter timestampConverter = new SecToMillisConverter();

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setMaxParallelism(1);

    NonaFlinkSerializerActivator.LINEAR_ROAD_FULL.activate(env, settings);

    DataStream<VehicleTupleGL> stopped =  env.addSource(KafkaOffsetSourceCreator.create(
                    settings.kafkaSourceTopic(),
                    settings.kafkaSourceBootstrapServer(),
                    new LinearRoadKafkaByteDeserializerGL(),
                    settings.queryID(),
                    settings.sourceLowerBound()))
        .name("LinearRoadSource"+settings.queryID())
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
        .returns(LinearRoadInputTupleGL.class)
        .filter(t -> t.getType() == 0 && t.getSpeed() == 0)
        .name("SpeedFilter"+settings.queryID())
        .keyBy(t -> t.getKey())
        .window(
            SlidingEventTimeWindows.of(STOPPED_VEHICLE_WINDOW_SIZE, STOPPED_VEHICLE_WINDOW_SLIDE))
        .aggregate(new VehicleAggregateGL(settings.aggregateStrategySupplier()))
        .name("VehicleAggregateGL"+settings.queryID())
        .returns(VehicleTupleGL.class)
//        .slotSharingGroup(settings.secondSlotSharingGroup())
        .filter(v -> v.getReports() == STOPPED_VEHICLE_MIN_STATIC_POSITIONS && v.isUniquePosition())
        .name("PositionFilter"+settings.queryID());

      settings
              .genealogActivator()
              .activate(
                      Arrays.asList(ProvenanceActivator.convert(stopped)),
                      Arrays.asList(settings.queryID()),
                      settings,
                      settings.queryU(),
                      timestampConverter,
                      env.getConfig()
              );

    env.execute("LinearRoadStoppedVehicles queryID" + settings.queryID());
  }
}
