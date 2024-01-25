package streamingRetention.usecases.carLocal;

import genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class CarLocalPedestriansToSingleObjectGL implements
    FlatMapFunction<CarLocalInputTupleGL, Tuple4GL<String, String, LidarImageContainer.Annotation3D, Long>> {

  @Override
  public void flatMap(CarLocalInputTupleGL t_in,
      Collector<Tuple4GL<String, String, LidarImageContainer.Annotation3D, Long>> collector)
      throws Exception {
    // annotations as such: object_ID : object_name, x, y, z
    Map<String, LidarImageContainer.Annotation3D> left_objects = t_in.f2.getAnnotations();
    String left_payload_type = t_in.f2.getPayloadType();
    for (Map.Entry<String, LidarImageContainer.Annotation3D> entry : left_objects.entrySet()) {
      Tuple4GL<String, String, LidarImageContainer.Annotation3D, Long> result = new Tuple4GL<>(
          left_payload_type, entry.getKey(), entry.getValue(), t_in.getStimulus());
      result.setTimestamp(t_in.getTimestamp());
      GenealogMapHelper.INSTANCE.annotateResult(t_in, result);
      collector.collect(result);
    }
    Map<String, LidarImageContainer.Annotation3D> right_objects = t_in.f3.getAnnotations();
    String right_payload_type = t_in.f3.getPayloadType();
    for (Map.Entry<String, LidarImageContainer.Annotation3D> entry : right_objects
        .entrySet()) {
      Tuple4GL<String, String, LidarImageContainer.Annotation3D, Long> result = new Tuple4GL<>(
          right_payload_type, entry.getKey(), entry.getValue(), t_in.getStimulus());
      result.setTimestamp(t_in.getTimestamp());
      GenealogMapHelper.INSTANCE.annotateResult(t_in, result);
      collector.collect(result);
    }
  }
}
