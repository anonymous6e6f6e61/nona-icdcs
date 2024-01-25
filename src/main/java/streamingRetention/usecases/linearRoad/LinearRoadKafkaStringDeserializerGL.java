package streamingRetention.usecases.linearRoad;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;


public class LinearRoadKafkaStringDeserializerGL implements DeserializationSchema<LinearRoadInputTupleGL> {


    @Override
    public LinearRoadInputTupleGL deserialize(byte[] message) {
        // when used as deserializer in a FlinkKafkaConsumer, "message" will be the "value" part of the Kafka record
        String messageAsString = new String(message);

        return LinearRoadInputTupleGL.fromReading(messageAsString);
    }

    @Override
    public boolean isEndOfStream(LinearRoadInputTupleGL t) {
        return false;
    }

    @Override
    public TypeInformation<LinearRoadInputTupleGL> getProducedType() {
        return TypeInformation.of(LinearRoadInputTupleGL.class);
    }
}
