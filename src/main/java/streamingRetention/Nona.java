package streamingRetention;

import ananke.output.ProvenanceGraphNode;
import ananke.output.ProvenanceGraphSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ExperimentSettings;
import util.TimestampConverter;

public class Nona {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final Logger LOG = LoggerFactory.getLogger(NonaSingleOperator.class);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        settings.serializerActivator().activate(env, settings);
        TimestampConverter timestampConverter = settings.timestampConverter();
        System.out.println(settings);

        // write marker tuple
        ArrayUnfoldedGenealogTuple marker = ArrayUnfoldedGenealogTuple.getMarker();
        try(KafkaKryoRecordSerializationSchema<ArrayUnfoldedGenealogTuple> serSchema =
                new KafkaKryoRecordSerializationSchema<>(0, "", env.getConfig(), ArrayUnfoldedGenealogTuple.class);) {
            serSchema.writeToFile(marker, settings.markerFilepath());
            LOG.info("Written marker tuple to " + settings.markerFilepath());
        }

        // begin query execution
        env
                .addSource(new NonaSingleOperator(settings, env.getConfig(), timestampConverter))
                .setParallelism(1)
                .returns(ProvenanceGraphNode.class)
                .name("NonaSingleOperator")
                .addSink(new ProvenanceGraphSink("SINK", settings, timestampConverter))
                .setParallelism(1)
                .name("ProvenanceGraphSink");

        env.execute("Nona");
    }
}
