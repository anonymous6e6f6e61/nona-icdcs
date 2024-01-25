package streamingRetention.playground.dummy;

import ananke.aggregate.ProvenanceAggregateStrategy;
import genealog.GenealogAccumulator;
import genealog.GenealogTupleType;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.function.Supplier;

public class DummyGLAggregate implements AggregateFunction<DummyGLTuple, DummyGLAggregate.DummyGLAccumulator, DummyGLTuple>  {

    private final Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier;

    public DummyGLAggregate(Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier) {
        this.aggregateStrategySupplier = aggregateStrategySupplier;
    }

    @Override
    public DummyGLAggregate.DummyGLAccumulator createAccumulator() {
        return new DummyGLAccumulator(aggregateStrategySupplier);
    }

    @Override
    public DummyGLAggregate.DummyGLAccumulator merge(
            DummyGLAggregate.DummyGLAccumulator acc1, DummyGLAggregate.DummyGLAccumulator acc2) {
        throw new UnsupportedOperationException("Merge not implemented for provenance windows!");
    }

    @Override
    public DummyGLTuple getResult(DummyGLAggregate.DummyGLAccumulator acc) {
        return acc.getAggregatedResult();
    }

    @Override
    public DummyGLAggregate.DummyGLAccumulator add(DummyGLTuple t, DummyGLAggregate.DummyGLAccumulator acc) {
        acc.add(t);
        return acc;
    }

    public static class DummyGLAccumulator extends GenealogAccumulator<DummyGLTuple, DummyGLTuple, DummyGLAccumulator> {

        private long timestamp;
        private long stimulus;

        public DummyGLAccumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
            super(strategySupplier);
        }

        @Override
        protected void doAdd(DummyGLTuple tuple) {
            timestamp = Math.max(timestamp, tuple.getTimestamp());
            stimulus = Math.max(stimulus, tuple.getStimulus());
        }

        @Override
        protected DummyGLTuple doGetAggregatedResult() {
            return new DummyGLTuple(timestamp, stimulus, -1f);
        }

        @Override
        protected void doMerge(DummyGLAccumulator other) {
            throw new UnsupportedOperationException("Lol, no support for this my friend.");
        }
    }

}

