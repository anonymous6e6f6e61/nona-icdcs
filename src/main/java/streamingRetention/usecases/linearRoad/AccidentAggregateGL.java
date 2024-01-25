package streamingRetention.usecases.linearRoad;

import ananke.aggregate.ProvenanceAggregateStrategy;
import genealog.GenealogAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import streamingRetention.usecases.CountTupleGL;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

public class AccidentAggregateGL
    implements AggregateFunction<VehicleTupleGL, AccidentAggregateGL.AccidentCountAccumulator, CountTupleGL> {

  private final Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier;

  public AccidentAggregateGL(
      Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier) {
    this.aggregateStrategySupplier = aggregateStrategySupplier;
  }

  @Override
  public AccidentCountAccumulator createAccumulator() {
    return new AccidentCountAccumulator(aggregateStrategySupplier);
  }

  @Override
  public AccidentCountAccumulator add(VehicleTupleGL value, AccidentCountAccumulator accumulator) {
    accumulator.add(value);
    return accumulator;
  }

  @Override
  public CountTupleGL getResult(AccidentCountAccumulator accumulator) {
    return accumulator.getAggregatedResult();
  }

  @Override
  public AccidentCountAccumulator merge(AccidentCountAccumulator a, AccidentCountAccumulator b) {
    throw new UnsupportedOperationException("Merge not implemented for provenance windows!");
  }

  public static class AccidentCountAccumulator
      extends GenealogAccumulator<VehicleTupleGL, CountTupleGL, AccidentCountAccumulator> {

    private final transient Set<Integer> carIds = new HashSet<>();
    private long timestamp = -1;
    private long stimulus;
    private String key;

    public AccidentCountAccumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
      super(strategySupplier);
    }

    @Override
    public void doAdd(VehicleTupleGL tuple) {
      timestamp = Math.max(timestamp, tuple.getTimestamp());
      stimulus = Math.max(stimulus, tuple.getStimulus());
      key = tuple.getKey();
      carIds.add(tuple.getVid());
    }

    @Override
    public CountTupleGL doGetAggregatedResult() {
      return new CountTupleGL(timestamp, key, stimulus, carIds.size());
    }

    @Override
    protected void doMerge(AccidentCountAccumulator other) {
      throw new UnsupportedOperationException("merge");
    }
  }
}
