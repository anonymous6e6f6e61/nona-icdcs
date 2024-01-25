package streamingRetention.usecases.carLocal;

import genealog.GenealogJoinHelper;
import org.apache.flink.api.common.functions.JoinFunction;

public class CarLocalPedestriansPredicateGL implements
    JoinFunction<Tuple4GL<String, String, LidarImageContainer.Annotation3D, Long>,
        Tuple4GL<String, String, LidarImageContainer.Annotation3D, Long>, CrossedPedestriansTuple> {

  @Override
  public CrossedPedestriansTuple join(
      Tuple4GL<String, String, LidarImageContainer.Annotation3D, Long> left,
      Tuple4GL<String, String, LidarImageContainer.Annotation3D, Long> right)
      throws Exception {
    CrossedPedestriansTuple result = new CrossedPedestriansTuple(left.f1, Math.max(left.f3, right.f3));
    result.setTimestamp(Math.max(left.getTimestamp(), right.getTimestamp()));
    result.setStimulus(result.f1);
    GenealogJoinHelper.INSTANCE.annotateResult(left, right, result);
    return result;
  }
}
