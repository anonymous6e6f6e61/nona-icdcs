package streamingRetention.usecases.linearRoad;

import ananke.aggregate.ProvenanceAggregateStrategy;
import genealog.GenealogAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.function.Supplier;

public class AVGVSaggregateGL implements AggregateFunction<
        LinearRoadInputTupleGL,
        AVGVSaggregateGL.SpeedAccumulator,
        LavTupleGL> {

    private final Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier;

    public AVGVSaggregateGL(Supplier<ProvenanceAggregateStrategy> strategy) {
        this.aggregateStrategySupplier = strategy;
    }

    @Override
    public SpeedAccumulator createAccumulator() {
        return new SpeedAccumulator(aggregateStrategySupplier);
    }

    @Override
    public SpeedAccumulator add(LinearRoadInputTupleGL tuple, SpeedAccumulator acc) {
        acc.add(tuple);
        return acc;
    }

    @Override
    public LavTupleGL getResult(SpeedAccumulator acc) {
        return acc.getAggregatedResult();
    }

    @Override
    public SpeedAccumulator merge(SpeedAccumulator acc1, SpeedAccumulator acc2) {
        throw new UnsupportedOperationException("Merge not implemented for provenance windows!");
    }

    public static class SpeedAccumulator extends GenealogAccumulator<
            LinearRoadInputTupleGL,
            LavTupleGL,
            SpeedAccumulator> {

        private long timestamp;
        private long stimulus;
        private int count;
        private double average_speed;
        private MultiKeyLR multikey;

        public SpeedAccumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
            super(strategySupplier);
        }

        @Override
        protected void doAdd(LinearRoadInputTupleGL tuple) {
            timestamp = Math.max(timestamp, tuple.getTimestamp());
            stimulus = Math.max(stimulus, tuple.getStimulus());
            multikey = MultiKeyLR.of(tuple.getSeg(), tuple.getDir(), tuple.getVid());
            average_speed = ( average_speed * count + tuple.getSpeed() ) / ( count + 1 );
            count++;
        }

        @Override
        protected LavTupleGL doGetAggregatedResult() {
            return new LavTupleGL(timestamp, multikey.toString(), stimulus, average_speed);
        }
    }
}
