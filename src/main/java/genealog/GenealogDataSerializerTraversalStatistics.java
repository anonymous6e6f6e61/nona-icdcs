package genealog;

import ananke.aggregate.ProvenanceAggregateStrategy;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import util.AvgStat;
import util.ExperimentSettings;
import util.TimestampedUIDTuple;

import java.util.Set;

public class GenealogDataSerializerTraversalStatistics extends GenealogDataSerializer {

  private static final String TASK_NAME = "SerializerTraverser";
  private String statisticsFolder;
  private transient AvgStat traversalStatistic;

  public GenealogDataSerializerTraversalStatistics(
      ProvenanceAggregateStrategy provenanceAggregateStrategy,
      String statisticsFolder) {
    super(provenanceAggregateStrategy);
    this.statisticsFolder = statisticsFolder;
  }

  public GenealogDataSerializerTraversalStatistics() {
    // For serialization/deserialization purposes
  }

  @Override
  public void write(Kryo kryo, Output output, GenealogData object) {
    initStatistic();
    super.write(kryo, output, object);
  }

  @Override
  protected Set<TimestampedUIDTuple> getProvenance(GenealogData object) {
    long start = System.nanoTime();
    Set<TimestampedUIDTuple> provenance = super.getProvenance(object);
    traversalStatistic.add(System.nanoTime() - start);
    traversalStatistic.flush();
    return provenance;
  }

  private void initStatistic() {
    if (traversalStatistic == null) {
      traversalStatistic =
          new AvgStat(ExperimentSettings.uniqueStatisticsFile(TASK_NAME, statisticsFolder,
              ExperimentSettings.TRAVERSAL_FILE, "csv"), true);
    }
  }

}
