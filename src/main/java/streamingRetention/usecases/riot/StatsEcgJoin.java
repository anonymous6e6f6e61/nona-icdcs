package streamingRetention.usecases.riot;

import genealog.GenealogJoinHelper;
import org.apache.flink.api.common.functions.JoinFunction;
import streamingRetention.usecases.carLocal.Tuple2GL;
import streamingRetention.usecases.carLocal.Tuple3GL;

public class StatsEcgJoin implements
        JoinFunction<Tuple2GL<Double, String>, Tuple2GL<Double, String>, Tuple3GL<Double, Double, String>> {

    @Override
    public Tuple3GL<Double, Double, String> join(Tuple2GL<Double, String> ecgLeft, Tuple2GL<Double, String> ecgRight)
            throws Exception {
        Tuple3GL<Double, Double, String> result = new Tuple3GL<>(ecgLeft.f0, ecgRight.f0, ecgLeft.f1);
        result.setTimestamp(Math.max(ecgLeft.getTimestamp(), ecgRight.getTimestamp()));
        result.setStimulus(Math.max(ecgLeft.getStimulus(), ecgRight.getStimulus()));
        GenealogJoinHelper.INSTANCE.annotateResult(ecgLeft, ecgRight, result);
        return result;
    }
}
