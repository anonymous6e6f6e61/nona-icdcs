package streamingRetention.usecases.riot;

import genealog.GenealogJoinHelper;
import org.apache.flink.api.common.functions.JoinFunction;
import streamingRetention.usecases.carLocal.Tuple3GL;

public class StatsFinalJoin implements
        JoinFunction<Tuple3GL<Double, Double, String>,
                Tuple7GL<Double, Double, Double, Double, Double, Double, String>, Tuple1GL<String>> {

    @Override
    public Tuple1GL<String> join(Tuple3GL<Double, Double, String> ecg,
                                 Tuple7GL<Double, Double, Double, Double, Double, Double, String> motion)
            throws Exception {
        Tuple1GL<String> result = new Tuple1GL<>(motion.f6);
        result.setTimestamp(Math.max(ecg.getTimestamp(), motion.getTimestamp()));
        result.setStimulus(Math.max(ecg.getStimulus(), motion.getStimulus()));
        GenealogJoinHelper.INSTANCE.annotateResult(ecg, motion, result);
        return result;
    }
}
