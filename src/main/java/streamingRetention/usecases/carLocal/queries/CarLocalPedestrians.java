package streamingRetention.usecases.carLocal.queries;

import java.util.Arrays;
import org.apache.flink.api.java.functions.KeySelector;
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

public class CarLocalPedestrians{

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

        DataStream<Tuple4GL<String, String, LidarImageContainer.Annotation3D, Long>> pedestrians = LidarLeftRightTimestamped
                .flatMap(
                        new CarLocalPedestriansToSingleObjectGL()) // payload_type, object_id, tuple4(object_name,x,y,z)
                .name("FlatMap"+settings.queryID())
                .filter(new CarLocalFilterPedestriansGL())
                .name("CarLocalFilterPedestriansGL"+settings.queryID());

        DataStream<CrossedPedestriansTuple> crossedPedestrians =
                pedestrians
                        .join(pedestrians)
                        .where(
                                (KeySelector<Tuple4GL<String, String, LidarImageContainer.Annotation3D, Long>, String>) left -> {
                                    if (left.f0.equals("ring_front_left")) {
                                        return "ring_front_right" + left.f1;
                                    } else {
                                        return "ring_front_left" + left.f1;
                                    }
                                })
                        .equalTo(right -> right.f0 + right.f1)
                        .window(SlidingEventTimeWindows
                                .of(PEDESTRIAN_PASSING_WINDOW_SIZE, PEDESTRIAN_PASSING_WINDOW_ADVANCE))
                        .apply(new CarLocalPedestriansPredicateGL());

        settings
                .genealogActivator()
                .activate(
                        Arrays.asList(
                                ProvenanceActivator.convert(crossedPedestrians)),
                        Arrays.asList(settings.queryID()),
                        settings,
                        settings.queryU(),
                        timestampConverter,
                        env.getConfig());

        env.execute("CarLocalPedestrians queryID" + settings.queryID());
    }

}

