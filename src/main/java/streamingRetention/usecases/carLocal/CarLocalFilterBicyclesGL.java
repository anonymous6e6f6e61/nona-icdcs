package streamingRetention.usecases.carLocal;

import org.apache.flink.api.common.functions.FilterFunction;

import static streamingRetention.usecases.carLocal.CarLocalConstants.CYCLIST_Y_AREA;
import static streamingRetention.usecases.carLocal.CarLocalConstants.CYCLIST_Z_AREA;


public class CarLocalFilterBicyclesGL implements
    FilterFunction<Tuple3GL<String, LidarImageContainer.Annotation3D, Long>> {

  @Override
  public boolean filter(
      Tuple3GL<String, LidarImageContainer.Annotation3D, Long> t)
      throws Exception {
    return (t.f1.labelClass.equals("BICYCLE")) && (-CYCLIST_Y_AREA < t.f1.x) && (t.f1.x
        < CYCLIST_Y_AREA)
        && (-CYCLIST_Z_AREA < t.f1.y) && (t.f1.y < CYCLIST_Z_AREA);
  }
}
