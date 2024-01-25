package streamingRetention.usecases.synthetic;

import genealog.GenealogTuple;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ExperimentSettings;
import util.TimestampedUIDTuple;
import java.io.Serializable;
import java.util.*;
import java.util.function.Supplier;

/**
 * Source that emits sink tuples which already contain (random) provenance.
 *
 * @param <T> The type of sink tuples to be emitted.
 */
public class SyntheticProvenanceSource<T extends GenealogTuple> extends
    RichParallelSourceFunction<T> {

  private static final Logger LOG = LoggerFactory.getLogger(SyntheticProvenanceSource.class);
  private final ExperimentSettings settings;
  private volatile boolean enabled;
  private final Supplier<T> tupleSupplier;
  private final Supplier<? extends TimestampedUIDTuple> provenanceSupplier;
  private transient Random random;
  private final long lowerBound;
  private transient AverageAccumulator realProvenanceOverlap;

  public <F extends Supplier<T> & Serializable> SyntheticProvenanceSource(
      F tupleSupplier,
      Supplier<? extends TimestampedUIDTuple> provenanceSupplier,
      ExperimentSettings settings) {
    this.settings = settings;
    this.tupleSupplier = tupleSupplier;
    this.provenanceSupplier = provenanceSupplier;
    this.lowerBound = settings.sourceLowerBound();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
  }

  T newSinkTuple(long timestamp, Collection<TimestampedUIDTuple> unsortedProvenance) throws Exception {
    // Sort provenance for consistency between executions
    final List<TimestampedUIDTuple> sortedProvenance = new ArrayList<>(unsortedProvenance);
    Collections.sort(sortedProvenance, Comparator.comparing(TimestampedUIDTuple::getUID));
    Collections.shuffle(sortedProvenance, random);
    final T sinkTuple = tupleSupplier.get();
    sinkTuple.setTimestamp(timestamp);
    final Collection<TimestampedUIDTuple> provenance = new HashSet<>();
    ArrayList<Long> timestampsAlreadyInProvenance = new ArrayList<>();
    for (TimestampedUIDTuple pTuple : sortedProvenance) {
      if (provenance.size() >= settings.synthetic_provenanceOverlap()) {
        break;
      }
      if (timestamp - pTuple.getTimestamp() <= settings.synthetic_windowSize()) {
        provenance.add(pTuple);
        timestampsAlreadyInProvenance.add(pTuple.getTimestamp());
      }
    }
    realProvenanceOverlap.add(provenance.size());
    final int remainingProvenanceSize =
        Math.max(0, settings.synthetic_provenanceSize() - provenance.size());
    ArrayList<Long> provenanceTimestamps = genRandomUniqueTimestamps(
            timestamp - settings.synthetic_windowSize(),
            timestamp - 1,
            remainingProvenanceSize,
            timestampsAlreadyInProvenance);
    for (long provTS : provenanceTimestamps) {
      TimestampedUIDTuple sourceTuple = provenanceSupplier.get();
      sourceTuple.setStimulus(System.currentTimeMillis());
      sourceTuple.setTimestamp(provTS);
      sourceTuple = (TimestampedUIDTuple) settings.genealogActivator().uidAssigner(0, settings.maxParallelism()).map(sourceTuple);
      provenance.add(sourceTuple);
    }
    sinkTuple.getGenealogData().setProvenance(provenance);
    return sinkTuple;
  }

  private void validateProvenanceOverlap() {
    if (settings.synthetic_provenanceOverlap() == 0) {
      return;
    }
    LOG.info(
        "Requested Overlap: {} | Average overlap: {}",
            settings.synthetic_provenanceOverlap(),
        realProvenanceOverlap.getLocalValue());
    double overlapRelativeDiff =
        100
            * (realProvenanceOverlap.getLocalValue() - settings.synthetic_provenanceOverlap())
            / settings.synthetic_provenanceOverlap();
    Validate.isTrue(
        Math.abs(overlapRelativeDiff) <= settings.synthetic_allowedOverlapDeviationPercent(),
        "Provenance overlap deviation is too high: %3.2f%% (Maximum allowed: %3.2f%%)",
        Math.abs(overlapRelativeDiff),
            settings.synthetic_allowedOverlapDeviationPercent());
  }

  @Override
  public void run(SourceContext<T> ctx) throws Exception {
    enabled = true;

    this.realProvenanceOverlap = new AverageAccumulator();
    this.random = new Random(0);
    Collection<TimestampedUIDTuple> previousProvenance = Collections.emptySet();
    long sinkTupleTimestamp = lowerBound + settings.synthetic_windowSize();
    int numInBatch = 0;
    while (enabled) {
      while (numInBatch < settings.synthetic_batchSize()) {
        T sinkTuple = newSinkTuple(sinkTupleTimestamp, previousProvenance);
        sinkTuple.setStimulus(System.currentTimeMillis());
        ctx.collect(sinkTuple);
        previousProvenance = sinkTuple.getProvenance();
        sinkTupleTimestamp++;
        numInBatch++;
      }
      numInBatch = 0;
      Thread.sleep(settings.synthetic_sleepBetweenBatches());
    }
    validateProvenanceOverlap();
  }

  @Override
  public void cancel() {
    enabled = false;
  }

  private ArrayList<Long> genRandomUniqueTimestamps(long lowerBound, long upperBound, int amount,
                                                    ArrayList<Long> toExclude) {
    if (lowerBound - upperBound > amount) {
      throw new IllegalArgumentException("Amount must be greater than distance between bounds!");
    }
    ArrayList<Long> allTimestamps = new ArrayList<>();
    for (long i=lowerBound; i<upperBound+1; i++) {
      if ( ! toExclude.contains(i) ) {
        allTimestamps.add(i);
      }
    }
    Collections.shuffle(allTimestamps);
    ArrayList<Long> selectedTimestamps = new ArrayList<>(amount);
    for (int i=0; i<amount; i++) {
      selectedTimestamps.add(allTimestamps.get(i));
    }
    return selectedTimestamps;
  }
}
