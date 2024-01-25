package streamingRetention.usecases.carLocal;

import org.apache.flink.api.common.functions.FilterFunction;

import static streamingRetention.usecases.carLocal.CarLocalConstants.CYCLIST_MIN_NUMBER;


public class CarLocalFilterCyclesInFrontGL implements
    FilterFunction<CyclesInFrontTuple> {

  @Override
  public boolean filter(CyclesInFrontTuple t) throws Exception {
    return t.f1 > CYCLIST_MIN_NUMBER;
  }
}
