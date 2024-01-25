package streamingRetention.usecases.linearRoad;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.ByteBuffer;


public class LinearRoadKafkaByteDeserializerGL implements DeserializationSchema<LinearRoadInputTupleGL> {


    @Override
    public LinearRoadInputTupleGL deserialize(byte[] message) {
        // when used as deserializer in a FlinkKafkaConsumer, "message" will be the "value" part of the Kafka record
        return LinearRoadInputTupleGL.fromReading(convertByteArrayToReading(message));
    }

    @Override
    public boolean isEndOfStream(LinearRoadInputTupleGL t) {
        return false;
    }

    @Override
    public TypeInformation<LinearRoadInputTupleGL> getProducedType() {
        return TypeInformation.of(LinearRoadInputTupleGL.class);
    }

    public static String convertByteArrayToReading(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        StringBuilder reading = new StringBuilder();
        reading.append(buffer.get());
        reading.append(",");
        reading.append(buffer.getInt());
        reading.append(",");
        reading.append(buffer.getInt());
        reading.append(",");
        reading.append(buffer.getInt());
        reading.append(",");
        reading.append(buffer.get());
        reading.append(",");
        reading.append(buffer.get());
        reading.append(",");
        reading.append(buffer.get());
        reading.append(",");
        reading.append(buffer.getInt());
        reading.append(",");
        reading.append(buffer.getInt());
        return reading.toString();
    }
}
