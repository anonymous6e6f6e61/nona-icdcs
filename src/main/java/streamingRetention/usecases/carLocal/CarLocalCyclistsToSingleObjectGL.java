package streamingRetention.usecases.carLocal;

import genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class CarLocalCyclistsToSingleObjectGL implements
    FlatMapFunction<CarLocalInputTupleGL, Tuple3GL<String, LidarImageContainer.Annotation3D, Long>> {

  @Override
  public void flatMap(CarLocalInputTupleGL t_in,
      Collector<Tuple3GL<String, LidarImageContainer.Annotation3D, Long>> collector)
      throws Exception {
    Map<String, LidarImageContainer.Annotation3D> lidarObjects = t_in.f1.getAnnotations();
    for (Map.Entry<String, LidarImageContainer.Annotation3D> entry : lidarObjects.entrySet()) {
      Tuple3GL<String, LidarImageContainer.Annotation3D, Long> result = new Tuple3GL<>(entry.getKey(), entry.getValue(),
          t_in.getStimulus());
      result.setTimestamp(t_in.getTimestamp());
      GenealogMapHelper.INSTANCE.annotateResult(t_in, result);
      collector.collect(result);
    }
  }
}
