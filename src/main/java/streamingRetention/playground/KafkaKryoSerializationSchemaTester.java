package streamingRetention.playground;

import genealog.GenealogTuple;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import streamingRetention.ArrayUnfoldedGenealogTuple;
import streamingRetention.KafkaKryoRecordSerializationSchema;
import streamingRetention.StringTupleUID;

public class KafkaKryoSerializationSchemaTester {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ExecutionConfig ec = env.getConfig();

        StringTupleUID sink = new StringTupleUID(100, "I am the sink.");
        int numSources = 10;
        GenealogTuple[] sources = new GenealogTuple[numSources];
        for (int i=0; i<numSources; i++) {
            sources[i] = (GenealogTuple) new StringTupleUID(10, "I am source " + i);
        }
        ArrayUnfoldedGenealogTuple aTuple = new ArrayUnfoldedGenealogTuple((GenealogTuple) sink, sources);
        KafkaKryoRecordSerializationSchema<ArrayUnfoldedGenealogTuple> serializationSchema =
                new KafkaKryoRecordSerializationSchema<>(0, "dummyTopic", ec, ArrayUnfoldedGenealogTuple.class);
        ProducerRecord<byte[],byte[]> serializedProdRecord = serializationSchema.serialize(aTuple, 100L);
        ConsumerRecord<byte[],byte[]> serializedConRecord =
                new ConsumerRecord<>("dummyTopic", 0, 0, serializedProdRecord.key(), serializedProdRecord.value());

        ArrayUnfoldedGenealogTuple bTuple = serializationSchema.deserialize(serializedConRecord);

        System.out.println("Before serialization: ");
        System.out.println(aTuple);

        System.out.println("After serialization: ");
        System.out.println(bTuple);

        env.execute();
    }
}
