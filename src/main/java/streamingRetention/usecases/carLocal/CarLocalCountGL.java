package streamingRetention.usecases.carLocal;

import ananke.aggregate.ProvenanceAggregateStrategy;
import genealog.GenealogAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import java.util.function.Supplier;

public class CarLocalCountGL implements
    AggregateFunction<Tuple3GL<String, LidarImageContainer.Annotation3D, Long>, CarLocalCountGL.Accumulator, CyclesInFrontTuple> {

  private final Supplier<ProvenanceAggregateStrategy> strategySupplier;

  public CarLocalCountGL(
      Supplier<ProvenanceAggregateStrategy> strategySupplier) {
    this.strategySupplier = strategySupplier;
  }

  @Override
  public Accumulator createAccumulator() {
    return new Accumulator(strategySupplier);
  }

  @Override
  public Accumulator add(Tuple3GL<String, LidarImageContainer.Annotation3D, Long> value,
      Accumulator accumulator) {
    accumulator.add(value);
    return accumulator;
  }

  @Override
  public CyclesInFrontTuple getResult(Accumulator accumulator) {
    return accumulator.getAggregatedResult();
  }

  @Override
  public Accumulator merge(Accumulator a, Accumulator b) {
    return null;
  }

  public static class Accumulator extends
          GenealogAccumulator<Tuple3GL<String, LidarImageContainer.Annotation3D, Long>, CyclesInFrontTuple, Accumulator> {

    private final CyclesInFrontTuple data = new CyclesInFrontTuple("", 0, 0L);

    public Accumulator(
        Supplier<ProvenanceAggregateStrategy> strategySupplier) {
      super(strategySupplier);
    }

    @Override
    protected void doAdd(Tuple3GL<String, LidarImageContainer.Annotation3D, Long> tuple) {
      data.f0 = tuple.f0;
      data.f1 += 1;
      data.f2 = Math.max(tuple.f2, data.f2);
      data.setTimestamp(tuple.getTimestamp());
    }

    @Override
    protected CyclesInFrontTuple doGetAggregatedResult() {
      data.setStimulus(data.f2);
      return data;
    }

  }
}
