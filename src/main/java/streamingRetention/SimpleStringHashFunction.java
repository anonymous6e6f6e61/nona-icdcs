package streamingRetention;

import java.io.Serializable;
import java.util.function.Function;

public class SimpleStringHashFunction implements Function<String, Long>, Serializable {
    @Override
    public Long apply(String s) {
        return (long) s.hashCode();
    }
}
