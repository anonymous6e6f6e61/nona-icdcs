package streamingRetention;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.TimestampedUIDTuple;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class KafkaSource<T extends TimestampedUIDTuple> extends RichSourceFunction<T> implements ResultTypeQueryable<T> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
    private final String inKafkaTopic;
    private final String kafkaBootstrapServer;
    private final DeserializationSchema<T> valueDeserializer;
    private final String queryID;
    private final long lowerBoundMilliseconds;
    private Properties properties;
    private Deserializer<T> deser;
    private boolean isRunning;
    private int currentBackoff;
    private final Class<T> typeParameterClass;


    public KafkaSource(String inKafkaTopic,
                       String kafkaBootstrapServer,
                       DeserializationSchema<T> deserializer,
                       String queryID,
                       long lowerBoundMilliseconds,
                       Class<T> typeParameterClass) {
        this.inKafkaTopic = inKafkaTopic;
        this.kafkaBootstrapServer = kafkaBootstrapServer;
        this.valueDeserializer = deserializer;
        this.queryID = queryID;
        this.lowerBoundMilliseconds = lowerBoundMilliseconds;
        this.typeParameterClass = typeParameterClass;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.properties = new Properties();
        String clientID = "consumer-" + queryID;
        properties.put("bootstrap.servers", kafkaBootstrapServer);
        properties.put("client.id", clientID);
        properties.put("group.id", queryID);
        isRunning = true;
    }


    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        isRunning = true;
        try (KafkaConsumer<String, T> sourceConsumer = new KafkaConsumer<String, T>(
                properties,
                new StringDeserializer(),
                new DeserializerFromDeserializationSchema<>(valueDeserializer))) {
            TopicPartition topicPartition = new TopicPartition(inKafkaTopic, 0);
            sourceConsumer.assign(Collections.singleton(topicPartition));
            sourceConsumer.seek(topicPartition, lowerBoundMilliseconds);
            resetBackoff();
            while (isRunning) {
                ConsumerRecords<String, T> records = sourceConsumer.poll(Duration.ofMillis(0));
                if (records.isEmpty()) {
                    increaseBackoff();
                } else {
                    resetBackoff();
                    for (ConsumerRecord<String, T> record : records) {
                        ctx.collectWithTimestamp(record.value(), record.timestamp());
                    }
                }
                backoff();
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private void backoff() throws InterruptedException {
        Thread.sleep(currentBackoff);
    }

    private void increaseBackoff() {
        int maxBackoff = 100;
        currentBackoff = Math.min(currentBackoff * 2, maxBackoff);
    }

    private void resetBackoff() {
        int baseBackoff = 1;
        currentBackoff = baseBackoff;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(typeParameterClass);
    }

    private class DeserializerFromDeserializationSchema<T2> implements Deserializer<T2> {
        private final DeserializationSchema<T2> deserializationSchema;

        public DeserializerFromDeserializationSchema(DeserializationSchema<T2> deserializationSchema) {
            this.deserializationSchema = deserializationSchema;
        }

        @Override
        public T2 deserialize(String s, byte[] bytes) {
            try {
                return deserializationSchema.deserialize(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            Deserializer.super.configure(configs, isKey);
        }

        @Override
        public T2 deserialize(String topic, Headers headers, byte[] bytes) {
            try {
                return deserializationSchema.deserialize(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            Deserializer.super.close();
        }
    }
}
