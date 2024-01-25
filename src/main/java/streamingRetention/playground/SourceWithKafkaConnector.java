package streamingRetention.playground;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.Properties;

//import KafkaProducer packages
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

//import ProducerRecord packages
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;

public class SourceWithKafkaConnector implements SourceFunction<Tuple1<String>> {
    // just a sample class that reacts to records from a Kafka topic and writes back to said Kafka topic

    private final String inTopicName  = "flinkReadsFromThis";
    private final String outTopicName = "flinkWritesToThis";
    private final int partition = 0;

    private String latestReadString;

    private final Properties consumerProps = new Properties();
    private final Properties producerProps = new Properties();
    private long offset;

    private boolean isRunning;

    public SourceWithKafkaConnector() {
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("group.id", "0");
        // .................................................................
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        isRunning = true;
    }


    @Override
    public void run(SourceContext<Tuple1<String>> ctx) throws Exception {

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(inTopicName));
        KafkaProducer<String,String> producer = new KafkaProducer<>(producerProps);


        latestReadString = "nothingReadFromKafkaYet";

        while (isRunning) {

            ConsumerRecords<String,String> readRecords = consumer.poll(Duration.ofMillis(0));
            // immediately return if there is nothing new in the topic
            if (readRecords.isEmpty()) {
                System.out.println("nothing new!");
            } else {
                for (ConsumerRecord<String, String> rec : readRecords) {
                    latestReadString = rec.value();
                    System.out.println(rec.timestamp());
                    System.out.println(System.currentTimeMillis());
                }
                producer.send(new ProducerRecord<>(outTopicName, "set latestReadString to "
                        + latestReadString));
            }

            ctx.collect(Tuple1.of(latestReadString));
            Thread.sleep(1);
        }
    }


    @Override
    public void cancel() {
        isRunning = false;
        System.out.println("Source closed.");
    }


}
