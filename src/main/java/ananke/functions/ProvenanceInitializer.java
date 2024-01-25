package ananke.functions;

import genealog.GenealogTupleType;
import org.apache.flink.api.common.functions.MapFunction;

import java.io.Serializable;
import java.util.function.Function;

public class ProvenanceInitializer<T> implements MapFunction<T, ProvenanceTupleContainer<T>> {

  private final Function<T, Long> timestampFunction;
  private final Function<T, Long> stimulusFunction;

  public <F extends Function<T, Long> & Serializable> ProvenanceInitializer(F timestampFunction,
      F stimulusFunction) {
    this.timestampFunction = timestampFunction;
    this.stimulusFunction = stimulusFunction;
  }

  @Override
  public ProvenanceTupleContainer<T> map(T value) throws Exception {
    ProvenanceTupleContainer<T> out =  new ProvenanceTupleContainer<>(value);
    out.initGenealog(GenealogTupleType.SOURCE);
    out.setTimestamp(timestampFunction.apply(value));
    out.setStimulus(stimulusFunction.apply(value));
    return out;
  }

}
