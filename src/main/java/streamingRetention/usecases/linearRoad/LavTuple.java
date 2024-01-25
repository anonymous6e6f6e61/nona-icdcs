package streamingRetention.usecases.linearRoad;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import util.BaseTuple;

import java.io.Serializable;
import java.util.Objects;

public class LavTuple extends BaseTuple {

    protected final double latest_average_velocity;

    public LavTuple(long timestamp, String key, long stimulus, double lav) {
        super(timestamp, key, stimulus);
        this.latest_average_velocity = lav;
    }

    public double getLAV() {
        return latest_average_velocity;
    }

    @Override
    public String toString() {
        return "LavTuple{" +
                "latest_average_velocity=" + latest_average_velocity +
                ", timestamp=" + timestamp +
                ", stimulus=" + stimulus +
                ", key='" + key + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        LavTuple that = (LavTuple) o;
        return Double.compare(that.latest_average_velocity, latest_average_velocity) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), latest_average_velocity);
    }

    public static class KryoSerializer extends Serializer<LavTuple> implements Serializable {

        @Override
        public void write(Kryo kryo, Output output, LavTuple object) {
            output.writeLong(object.getTimestamp());
            output.writeString(object.getKey());
            output.writeLong(object.getStimulus());
            output.writeDouble(object.getLAV());
        }

        @Override
        public LavTuple read(Kryo kryo, Input input, Class<LavTuple> type) {
            return new LavTuple(
                    input.readLong(), input.readString(), input.readLong(), input.readDouble());
        }
    }
}
