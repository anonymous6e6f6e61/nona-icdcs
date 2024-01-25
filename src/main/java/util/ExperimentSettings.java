package util;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import ananke.aggregate.ListAggregateStrategy;
import ananke.aggregate.ProvenanceAggregateStrategy;
import ananke.aggregate.SortedPointersAggregateStrategy;
import ananke.aggregate.UnsortedPointersAggregateStrategy;
import ananke.output.*;
import org.apache.commons.lang3.RandomStringUtils;
import streamingRetention.NonaFlinkSerializerActivator;
import streamingRetention.SecToMillisConverter;

import java.io.File;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class ExperimentSettings implements Serializable {

  public static final String LATENCY_FILE = "latency";
  public static final String THROUGHPUT_FILE = "rate";
  public static final String TRAVERSAL_FILE = "traversal";
  public static final String INPUT_EXTENSION = "txt";
  public static final String DEFAULT_SLOT_SHARING_GROUP = "default";
  public static final String SECOND_SLOT_SHARING_GROUP = "group2";
  private static final String THIRD_SLOT_SHARING_GROUP = "group3";
  private static final String PROVENANCE_READ_TIME = "provreadtime";
  private static final String PROVENANCE_WRITE_TIME = "provwritetime";
  private static final String PROVENANCE_READS = "provreads";
  private static final String PROVENANCE_WRITES = "provwrites";
  private static final String DELIVERY_LATENCY = "deliverylatency";
  private static final String TIME_LOGGER = "timelogger";
  private static final String WATERMARK_LOGGER = "watermark";
  private static final String THRESHOLD_LOGGER = "threshold";

  @Parameter(names = "--statisticsFolder", required = true, description = "path where output files will be stored")
  private String statisticsFolder;

  @Parameter(names = "--inputFile", description = "the input file of the streaming query")
  private String inputFile;

  @Parameter(names = "--outputFile", required = true, description = "the name of the file to store where the output of the query will be stored")
  private String outputFile;

  private int sourcesNumber = 1;

  @Parameter(names = "--autoFlush")
  private boolean autoFlush = true;

  @Parameter(names = "--distributed", description = "configure the query for distributed execution")
  private boolean distributed;

  @Parameter(names = "--traversalStatistics", description = "record GeneaLog graph traversal statistics")
  private boolean traversalStatistics;

  @Parameter(names = "--sourceRepetitions", description = "number of times to repeat the source input")
  private int sourceRepetitions = 1;

  @Parameter(names = "--idShift")
  private long idShift = 0;


  private int maxParallelism = 1;

  @Parameter(names = "--provenanceActivator", description = "provenance algorithm, e.g., ANANKE, GENEALOG, etc.")
  private ProvenanceActivator provenanceActivator = ProvenanceActivator.GENEALOG;

  @Parameter(names = "--aggregateStrategy", converter = AggregateStrategyConverter.class, description = "strategy for handling out-of-order aggregate tuples")
  private Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier =
      (Supplier<ProvenanceAggregateStrategy> & Serializable) UnsortedPointersAggregateStrategy::new;

  @Parameter(names = "--graphEncoder", description = "output encoder for the forward-provenance graph")
  private String graphEncoder = TimestampedFileProvenanceGraphEncoder.class.getSimpleName();

  @Parameter(names = "--disableSinkChaining")
  private boolean disableSinkChaining;

  @Parameter(names = "--kafkaSourceBootstrapServer", required = true,
          description = "IP:port of the Kafka bootstrap server for the source")
  private String kafkaSourceBootstrapServer;

  @Parameter(names = "--kafkaSinkBootstrapServer", description = "IP:port of the Kafka bootstrap server for the sink")
  private String kafkaSinkBootstrapServer = "localhost:9092";

  @Parameter(names = "--kafkaSourceTopic", description = "name of the Kafka source topic")
  private String kafkaSourceTopic = "SOURCE";

  @Parameter(names = "--kafkaSinkTopic", description = "name of the Kafka sink topic")
  private String kafkaSinkTopic = "PROVENANCE";

  @Parameter(names = "--kafkaRequestsTopic", description = "name of the Kafka transition requests topic")
  private String kafkaRequestsTopic = "REQUESTS";

  @Parameter(names = "--kafkaRepliesTopic", description = "name of the Kafka transition replies topic")
  private String kafkaRepliesTopic = "REPLIES";

  @Parameter(names = "--queryID", description = "a unique name for the query", required = true)
  private String queryID;

  @Parameter(names = "--sourceLowerBoundTimestamp", description = "the lower bound from which the source may " +
          "read tuples")
  private long sourceLowerBound = 0L;

  @Parameter(names = "--queryU", description = "the maximum lifetime of a tuple in the query", required = true)
  private long queryU;

  @Parameter(names = "--slackStrategy", description = "the chosen slack strategy")
  private String slackStrategy = "ZERO";

  @Parameter(names = "--serializerActivator", description = "the NonaFlinkSerializerActivator")
  private String serializerActivator = "";

  @Parameter(names = "--timestampConverter", description = "the timestamp converter for converting from tuple.getTimestamp() to context.timestamo()")
  private String timestampConverter = "";

  @Parameter(names = "--markerFilepath", description = "the filepath to store the marker at")
  private String markerFilepath = "";

  @DynamicParameter(names = "--preregisterQuery", description = "Pre-register queries in the format 'queryID=queryU'")
  private Map<String, String> preregisteredQueries = new HashMap<>();

  @Parameter(names = "--logWatermarks")
  private boolean logWatermarks;

  @Parameter(names = "--logThreshold")
  private boolean logThreshold;

  @Parameter(names = { "--synthetic-sleep-between-batches" }, description = "Synthetic: Sleep between batches in milliseconds")
  private int synthetic_sleepBetweenBatches = 2;

  @Parameter(names = { "--synthetic-batch-size" }, description = "Synthetic: Batch size")
  private int synthetic_batchSize = 4;

  @Parameter(names = { "--synthetic-allowed-overlap-deviation-percent" }, description = "Synthetic: Allowed overlap deviation percentage")
  private int synthetic_allowedOverlapDeviationPercent = 5;

  @Parameter(names = { "--synthetic-window-size" }, description = "Synthetic: Window size")
  private int synthetic_windowSize = 100;

  @Parameter(names = { "--synthetic-provenance-size" }, description = "Synthetic: Provenance size")
  private int synthetic_provenanceSize = 2;

  @Parameter(names = { "--synthetic-tuple-size" }, description = "Synthetic: Tuple size")
  private int synthetic_tupleSize = 8;

  @Parameter(names = { "--synthetic-provenance-overlap" }, description = "Synthetic: Provenance overlap")
  private int synthetic_provenanceOverlap = 0;

  public static ExperimentSettings newInstance(String[] args) {
    ExperimentSettings settings = new ExperimentSettings();
    JCommander.newBuilder().addObject(settings).build().parse(args);
    return settings;
  }

  public static String statisticsFile(
      String operator,
      Object taskIndex,
      String statisticsFolder,
      String filename,
      String fileExtension) {
    return new StringBuilder(statisticsFolder)
        .append(File.separator)
        .append(filename)
        .append("_")
        .append(operator)
        .append("_")
        .append(taskIndex)
        .append(".")
        .append(fileExtension)
        .toString();
  }

  public static String hostnameStatisticsFile(
      String operator,
      Object taskId,
      String statisticsFolder,
      String filename,
      String fileExtension) {
    String host = ManagementFactory.getRuntimeMXBean().getName();
    return statisticsFile(
        operator, String.format("%s_%s", host, taskId), statisticsFolder, filename, fileExtension);
  }

  public static String uniqueStatisticsFile(
      String operator, String statisticsFolder, String filename, String fileExtension) {
    String taskId = RandomStringUtils.randomAlphanumeric(10);
    return hostnameStatisticsFile(operator, taskId, statisticsFolder, filename, fileExtension);
  }

  public String secondSlotSharingGroup() {
    // If distributeHeavyOperators == false, assign all ops
    // to Flink's "default" co-location group (i.e., don't distribute to different slots)
    return distributed ? SECOND_SLOT_SHARING_GROUP : DEFAULT_SLOT_SHARING_GROUP;
  }

  public String thirdSlotSharingGroup() {
    // If distributeHeavyOperators == false, assign all ops
    // to Flink's "default" co-location group (i.e., don't distribute to different slots)
    return distributed ? THIRD_SLOT_SHARING_GROUP : DEFAULT_SLOT_SHARING_GROUP;
  }

  public boolean autoFlush() {
    return autoFlush;
  }

  public Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier() {
    return aggregateStrategySupplier;
  }

  public String inputFile() {
    return String.format("%s.%s", inputFile, INPUT_EXTENSION);
  }

  public String statisticsFolder() {
    return statisticsFolder;
  }


  public String latencyFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), LATENCY_FILE, "csv");
  }

  public String throughputFile(String operator, int taskIndex) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), THROUGHPUT_FILE, "csv");
  }

  public String outputFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), outputFile, "out");
  }

  public String provenanceReadsFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), PROVENANCE_READS, "csv");
  }

  public String provenanceWritesFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), PROVENANCE_WRITES, "csv");
  }

  public String provenanceReadTimeFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), PROVENANCE_READ_TIME, "csv");
  }

  public String provenanceWriteTimeFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), PROVENANCE_WRITE_TIME, "csv");
  }

  public String deliveryLatencyFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), DELIVERY_LATENCY, "csv");
  }

  public String timeLoggerFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), TIME_LOGGER, "csv");
  }

  public String watermarkLoggerFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), WATERMARK_LOGGER, "csv");
  }

  public String thresholdLoggerFile(int taskIndex, String operator) {
    return statisticsFile(operator, taskIndex, statisticsFolder(), THRESHOLD_LOGGER, "csv");
  }

  public long idShift() {
    return idShift;
  }

  public int sourcesNumber() {
    return sourcesNumber;
  }

  public int maxParallelism() {
    return maxParallelism;
  }

  public int sourceRepetitions() {
    return sourceRepetitions;
  }

  public int synthetic_sleepBetweenBatches() {
    return synthetic_sleepBetweenBatches;
  }

  public int synthetic_batchSize() {
    return synthetic_batchSize;
  }

  public int synthetic_allowedOverlapDeviationPercent() {
    return synthetic_allowedOverlapDeviationPercent;
  }

  public int synthetic_windowSize() {
    return synthetic_windowSize;
  }

  public int synthetic_provenanceSize() {
    return synthetic_provenanceSize;
  }

  public int synthetic_tupleSize() {
    return synthetic_tupleSize;
  }

  public int synthetic_provenanceOverlap() {
    return synthetic_provenanceOverlap;
  }

  public ProvenanceActivator genealogActivator() {
    return provenanceActivator;
  }

  public boolean graphTraversalStatistics() {
    return traversalStatistics;
  }


  public ProvenanceGraphEncoder newGraphEncoder(String name, int subtaskIndex) {

    if (FileProvenanceGraphEncoder.class.getSimpleName().equals(graphEncoder)) {
      return new FileProvenanceGraphEncoder(outputFile(subtaskIndex, name), autoFlush);
    } else if (TimestampedFileProvenanceGraphEncoder.class.getSimpleName().equals(graphEncoder)) {
      return new TimestampedFileProvenanceGraphEncoder(outputFile(subtaskIndex, name), autoFlush);
    } else if (NoOpProvenanceGraphEncoder.class.getSimpleName().equals(graphEncoder)) {
      return new NoOpProvenanceGraphEncoder();
    } else {
      throw new IllegalArgumentException(String.format("Invalid graph encoder: %s", graphEncoder));
    }

  }

  public NonaFlinkSerializerActivator serializerActivator() {
    switch (serializerActivator) {
      case ("LINEAR_ROAD_FULL"):
        return NonaFlinkSerializerActivator.LINEAR_ROAD_FULL;
      case ("CAR_LOCAL_FULL"):
        return NonaFlinkSerializerActivator.CAR_LOCAL_FULL;
      case ("RIOT_MHEALTH_FULL"):
        return NonaFlinkSerializerActivator.RIOT_MHEALTH_FULL;
      case ("SYNTHETIC"):
        return NonaFlinkSerializerActivator.SYNTHETIC;
      case ("MINIMUM"):
        return NonaFlinkSerializerActivator.MINIMUM;
      default:
        throw new IllegalArgumentException(
              String.format("Invalid serializerActivator: %s", serializerActivator));
    }
  }

  public TimestampConverter timestampConverter() {
    switch (timestampConverter) {
      case ("secToMillis"):
        return new SecToMillisConverter();
      case ("identity"):
        return new TimestampConverter() {
          @Override
          public Long apply(Long timestamp) {
            return timestamp;
          }
        };
      default:
        throw new IllegalArgumentException(
                String.format("Invalid timestampConverter: %s", serializerActivator));
    }
  }

  public String markerFilepath() {
    if (markerFilepath.equals("")) {
      throw new RuntimeException("Marker file path not set!");
    }
    return markerFilepath;
  }

  public boolean disableSinkChaining() {
    return disableSinkChaining;
  }

  public String kafkaSourceBootstrapServer() {
    return kafkaSourceBootstrapServer;
  }

  public String kafkaSinkBootstrapServer() {
    return kafkaSinkBootstrapServer;
  }

  public String kafkaSourceTopic() {
    return kafkaSourceTopic;
  }

  public String kafkaSinkTopic() {
    return kafkaSinkTopic;
  }

  public String queryID() {
    return queryID;
  }

  public long sourceLowerBound() {
    return sourceLowerBound;
  }

  public long queryU() {
    return queryU;
  }

  public SlackStrategy slackStrategy() {
    switch (slackStrategy) {
      case "ZERO":
        return SlackStrategy.ZERO;
      case "TW":
        return SlackStrategy.TW;
      case "T":
        return SlackStrategy.T;
      default:
        throw new IllegalArgumentException("Unknown SlackStrategy provided: " + slackStrategy);
    }
  }

  public String kafkaRequestsTopic() {
    return kafkaRequestsTopic;
  }

  public String kafkaRepliesTopic() {
    return kafkaRepliesTopic;
  }

  private static class AggregateStrategyConverter
      implements IStringConverter<Supplier<ProvenanceAggregateStrategy>> {

    @Override
    public Supplier<ProvenanceAggregateStrategy> convert(String s) {
      switch (s.trim()) {
        case "unsortedPtr":
          return (Supplier<ProvenanceAggregateStrategy> & Serializable)
              UnsortedPointersAggregateStrategy::new;
        case "sortedPtr":
          return (Supplier<ProvenanceAggregateStrategy> & Serializable)
              SortedPointersAggregateStrategy::new;
        case "list":
          return (Supplier<ProvenanceAggregateStrategy> & Serializable) ListAggregateStrategy::new;
        default:
          throw new IllegalArgumentException("Unknown GeneaLog aggregate strategy provided");
      }
    }
  }

  public Map<String, String> preregisteredQueries() {
    if (preregisteredQueries.isEmpty()) {
      throw new IllegalArgumentException("No queries pre-registered!");
    }
    return preregisteredQueries;
  }

  public boolean logWatermarks() {
    return logWatermarks;
  }

  public boolean logThreshold() {
    return logThreshold;
  }

  @Override
  public String toString() {
    return "ExperimentSettings{" +
            "statisticsFolder='" + statisticsFolder + '\'' +
            ", inputFile='" + inputFile + '\'' +
            ", outputFile='" + outputFile + '\'' +
            ", sourcesNumber=" + sourcesNumber +
            ", autoFlush=" + autoFlush +
            ", distributed=" + distributed +
            ", traversalStatistics=" + traversalStatistics +
            ", sourceRepetitions=" + sourceRepetitions +
            ", idShift=" + idShift +
            ", provenanceActivator=" + provenanceActivator +
            ", aggregateStrategySupplier=" + aggregateStrategySupplier +
            ", graphEncoder='" + graphEncoder + '\'' +
            ", disableSinkChaining=" + disableSinkChaining +
            ", kafkaSourceBootstrapServer='" + kafkaSourceBootstrapServer + '\'' +
            ", kafkaSinkBootstrapServer='" + kafkaSinkBootstrapServer + '\'' +
            ", kafkaSourceTopic='" + kafkaSourceTopic + '\'' +
            ", kafkaSinkTopic='" + kafkaSinkTopic + '\'' +
            ", kafkaRequestsTopic='" + kafkaRequestsTopic + '\'' +
            ", kafkaRepliesTopic='" + kafkaRepliesTopic + '\'' +
            ", queryID='" + queryID + '\'' +
            ", sourceLowerBound=" + sourceLowerBound +
            ", queryU=" + queryU +
            ", slackStrategy='" + slackStrategy + '\'' +
            ", serializerActivator='" + serializerActivator + '\'' +
            ", timestampConverter='" + timestampConverter + '\'' +
            ", markerFilepath='" + markerFilepath + '\'' +
            ", preregisteredQueries=" + preregisteredQueries +
            ", logWatermarks=" + logWatermarks +
            ", logThreshold=" + logThreshold +
            ", synthetic_sleepBetweenBatches=" + synthetic_sleepBetweenBatches +
            ", synthetic_batchSize=" + synthetic_batchSize +
            ", synthetic_allowedOverlapDeviationPercent=" + synthetic_allowedOverlapDeviationPercent +
            ", synthetic_windowSize=" + synthetic_windowSize +
            ", synthetic_provenanceSize=" + synthetic_provenanceSize +
            ", synthetic_tupleSize=" + synthetic_tupleSize +
            ", synthetic_provenanceOverlap=" + synthetic_provenanceOverlap +
            '}';
  }
}
