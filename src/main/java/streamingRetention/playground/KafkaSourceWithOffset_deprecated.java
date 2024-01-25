package streamingRetention.playground;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class KafkaSourceWithOffset_deprecated<T,K,V> implements SourceFunction<T> {

    private String inKafkaTopic;
    private String kafkaBootstrapServer;
    private String kafkaKeyDeserializer;
    private String kafkaValueDeserializer;
    private int offset;
    private final Properties consumerProps = new Properties();


    public KafkaSourceWithOffset_deprecated(String inKafkaTopic, String kafkaBootstrapServer, String kafkaKeyDeserializer,
                                            String kafkaValueSerializer, int offset) {
        this.inKafkaTopic = inKafkaTopic;
        this.kafkaBootstrapServer = kafkaBootstrapServer;
        this.kafkaKeyDeserializer = kafkaKeyDeserializer;
        this.kafkaValueDeserializer = kafkaValueSerializer;
        this.offset = offset;

        consumerProps.put("bootstrap.servers", kafkaBootstrapServer);
        consumerProps.put("key.deserializer", kafkaKeyDeserializer);
        consumerProps.put("value.deserializer", kafkaValueDeserializer);
        consumerProps.put("group.id", "0");
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        try (KafkaConsumer<K,V> consumer = new KafkaConsumer<K, V>(consumerProps)) {

        }
    }

    @Override
    public void cancel() {

    }
}
