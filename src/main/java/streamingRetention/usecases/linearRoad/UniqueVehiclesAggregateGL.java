package streamingRetention.usecases.linearRoad;

import ananke.aggregate.ProvenanceAggregateStrategy;
import genealog.GenealogAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import streamingRetention.usecases.CountTupleGL;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class UniqueVehiclesAggregateGL
    implements AggregateFunction<
        LinearRoadInputTupleGL,
        UniqueVehiclesAggregateGL.UniqueAccumulator,
        CountTupleGL> {

    private final Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier;

    public UniqueVehiclesAggregateGL(Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier) {
        this.aggregateStrategySupplier = aggregateStrategySupplier;
    }

    @Override
    public UniqueAccumulator createAccumulator() {
        return new UniqueAccumulator(aggregateStrategySupplier);
    }

    @Override
    public UniqueAccumulator add(LinearRoadInputTupleGL tuple, UniqueAccumulator acc) {
        acc.add(tuple);
        return acc;
    }

    @Override
    public CountTupleGL getResult(UniqueAccumulator uniqueAccumulator) {
        return uniqueAccumulator.getAggregatedResult();
    }

    @Override
    public UniqueAccumulator merge(UniqueAccumulator uniqueAccumulator, UniqueAccumulator acc1) {
        throw new UnsupportedOperationException("Merge not implemented for provenance windows!");
    }

    public static class UniqueAccumulator
        extends GenealogAccumulator<LinearRoadInputTupleGL, CountTupleGL, UniqueAccumulator> {

        private final transient List<Long> uniqueVehicleIDs = new ArrayList<>();
        private long timestamp = -1;
        private MultiKeyLR multikey;
        private long stimulus;

        public UniqueAccumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
            super(strategySupplier);
        }

        @Override
        public void doAdd(LinearRoadInputTupleGL tuple) {
            uniqueVehicleIDs.add(tuple.getVid());
            timestamp = Math.max(timestamp, tuple.getTimestamp());
            stimulus = Math.max(stimulus, tuple.getStimulus());
            multikey = MultiKeyLR.of(tuple.getSeg(), tuple.getDir());
        }

        @Override
        public CountTupleGL doGetAggregatedResult() {
            return new CountTupleGL(timestamp, multikey.toString(), stimulus, uniqueVehicleIDs.size());
        }


    }
}
