package streamingRetention.usecases.linearRoad;

import java.nio.ByteBuffer;

public class LinearRoadReadingUtil {
    public static byte[] getBytesFromReading(String reading, int ts) {
        String[] words = reading.split(",");
        ByteBuffer buffer = ByteBuffer.allocate(4 * Byte.BYTES + 5 * Integer.BYTES);
        buffer.put(Byte.parseByte(words[0]));
        buffer.putInt(ts);
        buffer.putInt(Integer.parseInt(words[2]));
        buffer.putInt(Integer.parseInt(words[3]));
        buffer.put(Byte.parseByte(words[4]));
        buffer.put(Byte.parseByte(words[5]));
        buffer.put(Byte.parseByte(words[6]));
        buffer.putInt(Integer.parseInt(words[7]));
        buffer.putInt(Integer.parseInt(words[8]));

        return buffer.array();
    }
}
