package streamingRetention.usecases.riot;


import ananke.aggregate.ProvenanceAggregateStrategy;
import genealog.GenealogAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import streamingRetention.usecases.carLocal.Tuple2GL;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

public class StatsEcg1Average
        implements AggregateFunction<MhealthInputTupleGL, StatsEcg1Average.StatsAverageAccumulator,
        Tuple2GL<Double, String>> {

    private final Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier;

    public StatsEcg1Average(
            Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier) {
        this.aggregateStrategySupplier = aggregateStrategySupplier;
    }

    @Override
    public StatsAverageAccumulator createAccumulator() {
        return new StatsAverageAccumulator(aggregateStrategySupplier);
    }

    @Override
    public StatsAverageAccumulator add(MhealthInputTupleGL value, StatsAverageAccumulator accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public Tuple2GL<Double, String> getResult(StatsAverageAccumulator accumulator) {
        return accumulator.getAggregatedResult();
    }

    @Override
    public StatsAverageAccumulator merge(StatsAverageAccumulator a, StatsAverageAccumulator b) {
        throw new UnsupportedOperationException("Merge not implemented for provenance windows!");
    }

    public static class StatsAverageAccumulator
            extends GenealogAccumulator<MhealthInputTupleGL, Tuple2GL<Double, String>, StatsAverageAccumulator> {

        private final transient Set<Double> ecgReadings = new HashSet<>();
        private long timestamp = -1;
        private long stimulus;
        private String key;

        public StatsAverageAccumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
            super(strategySupplier);
        }

        @Override
        public void doAdd(MhealthInputTupleGL tuple) {
            timestamp = Math.max(timestamp, tuple.getTimestamp());
            stimulus = Math.max(stimulus, tuple.getStimulus());
            key = tuple.getKey();
            ecgReadings.add(tuple.getEcgSignalLead1());
        }

        @Override
        public Tuple2GL<Double, String> doGetAggregatedResult() {
            double average = 0;
            for (double ecgReading : ecgReadings) {
                average += ecgReading;
            }
            average = average / ecgReadings.size();
            Tuple2GL<Double, String> out = new Tuple2GL<>(average, key);
            out.setTimestamp(timestamp);
            out.setStimulus(stimulus);
            return out;
        }

        @Override
        protected void doMerge(StatsAverageAccumulator other) {
            throw new UnsupportedOperationException("merge");
        }
    }
}
