package ananke.functions;

import ananke.aggregate.ProvenanceAggregateStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.io.Serializable;
import java.util.function.Supplier;

public class ProvenanceAggregateFunction<IN, ACC, OUT>
    implements AggregateFunction<
    ProvenanceTupleContainer<IN>, ProvenanceAggregateFunction.GenealogMetadataAccumulator<ACC>, ProvenanceTupleContainer<OUT>> {

  private final AggregateFunction<IN, ACC, OUT> delegate;
  private final Supplier<ProvenanceAggregateStrategy> strategySupplier;

  public static class GenealogMetadataAccumulator<T> implements Serializable {
    private final ProvenanceAggregateStrategy strategy;
    T accumulator;
    private long timestamp;
    private long stimulus;

    public GenealogMetadataAccumulator(ProvenanceAggregateStrategy strategy, T accumulator) {
      this.strategy = strategy;
      this.accumulator = accumulator;
    }

    void updateTimestamp(long timestamp) {
      this.timestamp = Math.max(this.timestamp, timestamp);
    }

    void updateStimulus(long stimulus) {
      this.stimulus = Math.max(this.stimulus, stimulus);
    }
  }

  public ProvenanceAggregateFunction(
      Supplier<ProvenanceAggregateStrategy> strategySupplier,
      AggregateFunction<IN, ACC, OUT> delegate) {
    this.delegate = delegate;
    this.strategySupplier = strategySupplier;
  }

  @Override
  public GenealogMetadataAccumulator<ACC> createAccumulator() {
    return new GenealogMetadataAccumulator<>(strategySupplier.get(), delegate.createAccumulator());
  }

  @Override
  public GenealogMetadataAccumulator<ACC> add(
      ProvenanceTupleContainer<IN> value, GenealogMetadataAccumulator<ACC> accumulator) {
    accumulator.strategy.addWindowProvenance(value);
    accumulator.updateTimestamp(value.getTimestamp());
    accumulator.updateStimulus(value.getStimulus());
    accumulator.accumulator = delegate.add(value.tuple(), accumulator.accumulator);
    return accumulator;
  }

  @Override
  public ProvenanceTupleContainer<OUT> getResult(GenealogMetadataAccumulator<ACC> accumulator) {
    OUT result = delegate.getResult(accumulator.accumulator);
    ProvenanceTupleContainer genealogResult = new ProvenanceTupleContainer(result);
    accumulator.strategy.annotateWindowResult(genealogResult);
    genealogResult.setStimulus(accumulator.stimulus);
    genealogResult.setTimestamp(accumulator.timestamp);
    return genealogResult;
  }

  @Override
  public GenealogMetadataAccumulator<ACC> merge(
      GenealogMetadataAccumulator<ACC> a, GenealogMetadataAccumulator<ACC> b) {
    throw new UnsupportedOperationException("merge");
  }
}
