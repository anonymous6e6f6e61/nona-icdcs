package streamingRetention;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class KafkaStringSerializationSchema implements KafkaSerializationSchema<ArrayUnfoldedGenealogTuple> {

    int partition;
    String kafkaTopic;

    public KafkaStringSerializationSchema(int partition, String kafkaTopic) {
        this.partition = partition;
        this.kafkaTopic = kafkaTopic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(ArrayUnfoldedGenealogTuple t, @Nullable Long timestamp) {
        byte[] value = t.toString().getBytes(StandardCharsets.UTF_8);

        return new ProducerRecord<>(kafkaTopic, partition, timestamp, null, value);
    }


}

