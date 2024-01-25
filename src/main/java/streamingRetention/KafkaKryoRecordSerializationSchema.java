package streamingRetention;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class KafkaKryoRecordSerializationSchema<T> implements
        KafkaSerializationSchema<T>,
        KafkaDeserializationSchema<T>,
        Deserializer<T> {

    private final int partition;
    private final String kafkaTopic;
    private final Class<T> clazz;
    private final int DATA_OUTPUT_SERIALIZER_START_SIZE = 100;
    private final KryoSerializer<T> serializer;
    private transient DataOutputSerializer outputSerializer =
            new DataOutputSerializer(DATA_OUTPUT_SERIALIZER_START_SIZE);
    private final DataInputDeserializer inputDeserializer =
            new DataInputDeserializer();

    public KafkaKryoRecordSerializationSchema(int partition, String kafkaTopic, ExecutionConfig ec, Class<T> clazz) {
        this.partition = partition;
        this.kafkaTopic = kafkaTopic;
        this.clazz = clazz;
        this.serializer = new KryoSerializer<>(clazz, ec);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T t, @Nullable Long timestamp) {
        if (outputSerializer == null) {
            outputSerializer = new DataOutputSerializer(DATA_OUTPUT_SERIALIZER_START_SIZE);
        }
        outputSerializer.clear();
        try {
            serializer.serialize(t, outputSerializer);
        } catch (Exception e) {
            throw new RuntimeException("Exception encountered while serializing tuple " + t +
                    ". Encountered exception: " + e);
        }
        // TODO: remove following when appropriate
//        if (timestamp != null & timestamp < 0) {
//            throw new IllegalArgumentException("Encountered illegal timestamp that Kafka will not accept: "
//                    + timestamp + " when serializing tuple " + t);
//        }
        return new ProducerRecord<>(kafkaTopic, partition, null, outputSerializer.getCopyOfBuffer());
    }

    @Override
    public boolean isEndOfStream(T t) {
        return false;
    }

    @Override
    public T deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
        return deserialize(null, consumerRecord.value());
    }

    public T deserialize(String topic, byte[] bytes) {
        inputDeserializer.setBuffer(bytes);
        try {
            return serializer.deserialize(inputDeserializer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(clazz);
    }

    public void writeToFile(T tuple, String filepath) {
        if (outputSerializer == null) {
            outputSerializer = new DataOutputSerializer(DATA_OUTPUT_SERIALIZER_START_SIZE);
        }
        outputSerializer.clear();
        try(FileOutputStream fileOutputStream = new FileOutputStream(new File(filepath), false);) {
            serializer.serialize(tuple, outputSerializer);
            fileOutputStream.write(outputSerializer.getCopyOfBuffer());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
