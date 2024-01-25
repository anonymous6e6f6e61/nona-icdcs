package streamingRetention.usecases.riot;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class MhealthStringDeserializerGL implements DeserializationSchema<MhealthInputTupleGL> {

    @Override
    public MhealthInputTupleGL deserialize(byte[] bytes) throws IOException {
        String reading = new String(bytes);
        return MhealthInputTupleGL.fromReading(reading);
    }

    @Override
    public boolean isEndOfStream(MhealthInputTupleGL mhealthInputTupleGL) {
        return false;
    }

    @Override
    public TypeInformation<MhealthInputTupleGL> getProducedType() {
        return TypeInformation.of(MhealthInputTupleGL.class);
    }
}
