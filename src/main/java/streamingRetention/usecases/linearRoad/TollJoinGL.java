package streamingRetention.usecases.linearRoad;

import genealog.GenealogJoinHelper;
import org.apache.flink.api.common.functions.JoinFunction;
import streamingRetention.usecases.CountTupleGL;


public class TollJoinGL implements JoinFunction<CountTupleGL, LavTupleGL, TollTupleGL> {

    @Override
    public TollTupleGL join(CountTupleGL left, LavTupleGL right) throws Exception {
        long timestamp = Math.max(left.getTimestamp(), right.getTimestamp());
        long stimulus = Math.max(left.getStimulus(), right.getStimulus());
        String key = left.getKey();
        TollTupleGL result = new TollTupleGL(timestamp, key, stimulus);
        GenealogJoinHelper.INSTANCE.annotateResult(left, right, result);
        return result;
    }
}
