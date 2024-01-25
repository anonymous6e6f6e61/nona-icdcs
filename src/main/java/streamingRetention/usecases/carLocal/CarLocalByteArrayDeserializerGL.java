package streamingRetention.usecases.carLocal;

import genealog.GenealogTupleType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class CarLocalByteArrayDeserializerGL implements DeserializationSchema<CarLocalInputTupleGL> {

    public LidarImageContainer[] convertToObject(byte[] byteArray)
            throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
        ObjectInputStream ois = new ObjectInputStream(bis);
        LidarImageContainer[] object = (LidarImageContainer[]) ois.readObject();
        ois.close();
        bis.close();
        return object;
    }

    @Override
    public CarLocalInputTupleGL deserialize(byte[] message) throws IOException {
        // when used as deserializer in a FlinkKafkaConsumer, "message" will be the "value" part of the Kafka record
        LidarImageContainer[] receivedTuple;
        try {
            receivedTuple = convertToObject(message);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        long timestamp = receivedTuple[0].getTimestamp();
        LidarImageContainer lidar = receivedTuple[0];
        LidarImageContainer left  = receivedTuple[2];
        LidarImageContainer right = receivedTuple[2];
        CarLocalInputTupleGL tupleGL = new CarLocalInputTupleGL(timestamp, lidar, left,
                right);
        tupleGL.initGenealog(GenealogTupleType.SOURCE);
        return tupleGL;
    }

    @Override
    public boolean isEndOfStream(CarLocalInputTupleGL carLocalInputTupleGL) {
        return false;
    }

    @Override
    public TypeInformation<CarLocalInputTupleGL> getProducedType() {
        return TypeInformation.of(CarLocalInputTupleGL.class);
    }
}
