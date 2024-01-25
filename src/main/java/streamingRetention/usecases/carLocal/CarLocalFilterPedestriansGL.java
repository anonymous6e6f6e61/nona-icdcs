package streamingRetention.usecases.carLocal;

import org.apache.flink.api.common.functions.FilterFunction;

public class CarLocalFilterPedestriansGL implements
    FilterFunction<Tuple4GL<String, String, LidarImageContainer.Annotation3D, Long>> {

  @Override
  public boolean filter(
      Tuple4GL<String, String, LidarImageContainer.Annotation3D, Long> t)
      throws Exception {
    return t.f2.labelClass.equals("PEDESTRIAN");
  }
}
