package streamingRetention;

import ananke.output.ProvenanceGraphNode;
import ananke.output.ProvenanceGraphSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.ExperimentSettings;
import util.TimestampConverter;

public class AnankeK {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        settings.serializerActivator().activate(env, settings);
        System.out.println(settings);

        TimestampConverter timestampConverter = settings.timestampConverter();

        env
                .addSource(new AnankeKafkaOperator(settings, env.getConfig(), timestampConverter))
                .setParallelism(1)
                .returns(ProvenanceGraphNode.class)
                .name("AnankeKSingleOperator")
                .addSink(new ProvenanceGraphSink("SINK", settings, timestampConverter))
                .setParallelism(1)
                .name("ProvenanceGraphSink");

        env.execute("AnankeK");
    }
}
