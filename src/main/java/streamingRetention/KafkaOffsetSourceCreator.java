package streamingRetention;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

public class KafkaOffsetSourceCreator {

    public static<T> FlinkKafkaConsumerBase<T> create (String inKafkaTopic,
                                                       String kafkaBootstrapServer,
                                                       DeserializationSchema<T> deserializer,
                                                       String queryID,
                                                       long lowerBoundMilliseconds) {
        Properties properties = new Properties();

        String clientID = "consumer-" + queryID;
        properties.put("bootstrap.servers", kafkaBootstrapServer);
        properties.put("client.id", clientID);
        properties.put("group.id", queryID);

        return new FlinkKafkaConsumer<>(inKafkaTopic, deserializer, properties)
                .setStartFromTimestamp((long) Math.ceil((double) lowerBoundMilliseconds / 1000));
    }
}
