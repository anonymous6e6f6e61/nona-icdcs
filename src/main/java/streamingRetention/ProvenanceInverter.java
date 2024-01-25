package streamingRetention;

import ananke.output.ProvenanceGraphNode;
import util.*;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ProvenanceInverter extends
        RichFlatMapFunction<AnnotatedArrayUnfoldedGenealogTuple, ProvenanceGraphNode>
        implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ProvenanceInverter.class);
    public static final String EXTRACTOR_STAT_NAME = "NONA";
    private final ExperimentSettings settings;
    private final TimestampConverter timestampConverter;
    private long threshold = 0;
    private NavigableSet<ProvenanceKey> pendingSourceAcks;
    private transient AvgStat provenanceWriteTimeStatistic;
    private transient AvgStat provenanceReadTimeStatistic;
    private transient CountStat provenanceReadsStatistic;
    private transient CountStat provenanceWritesStatistic;
    private transient MaxStat deliveryLatencyStatistic;

    public ProvenanceInverter(TimestampConverter timestampConverter, ExperimentSettings settings) {
        Validate.notNull(timestampConverter);
        Validate.notNull(settings);
        this.timestampConverter = timestampConverter;
        this.settings = settings;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.warn("Unique IDs required in the tuples for forward provenance to work correctly! See: {}",
                IncreasingUIDGenerator.class.getName());
        final int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        this.provenanceWriteTimeStatistic =
                new AvgStat(settings.provenanceWriteTimeFile(indexOfThisSubtask, EXTRACTOR_STAT_NAME),
                        settings.autoFlush());
        this.provenanceReadTimeStatistic =
                new AvgStat(settings.provenanceReadTimeFile(indexOfThisSubtask, EXTRACTOR_STAT_NAME),
                        settings.autoFlush());
        this.provenanceWritesStatistic = new CountStat(
                settings.provenanceWritesFile(indexOfThisSubtask, EXTRACTOR_STAT_NAME),
                settings.autoFlush());
        this.provenanceReadsStatistic = new CountStat(
                settings.provenanceReadsFile(indexOfThisSubtask, EXTRACTOR_STAT_NAME),
                settings.autoFlush());
        this.deliveryLatencyStatistic = new MaxStat(
                settings.deliveryLatencyFile(indexOfThisSubtask, EXTRACTOR_STAT_NAME),
                settings.autoFlush());
        this.pendingSourceAcks = new TreeSet<>();
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        provenanceReadTimeStatistic.close();
        provenanceWriteTimeStatistic.close();
        provenanceReadsStatistic.close();
        provenanceWritesStatistic.close();
        super.close();
    }

    @Override
    public void flatMap(AnnotatedArrayUnfoldedGenealogTuple tuple, Collector<ProvenanceGraphNode> out) {
        final long start = System.currentTimeMillis();
        if (tuple.isThresholdChanged()) {
            threshold = tuple.getThreshold();
        }
        TimestampedUIDTuple sinkTuple = tuple.getSinkTuple();
        TimestampedUIDTuple[] sourceTuples = tuple.getSourceTuples();
        out.collect(ProvenanceGraphNode.newSink(sinkTuple));
        for (TimestampedUIDTuple sourceTuple : sourceTuples) {
            ProvenanceKey sourceTupleKey = ProvenanceKey.ofTuple(sourceTuple, timestampConverter, start);
            if (!pendingSourceAcks.contains(sourceTupleKey)) {
                out.collect(ProvenanceGraphNode.newSource(sourceTuple));
                pendingSourceAcks.add(sourceTupleKey);
            }
            out.collect(ProvenanceGraphNode.newEdge(sourceTuple.getUID(), sinkTuple.getUID()));
        }
        out.collect(ProvenanceGraphNode.newACK(sinkTuple.getUID()));
        final long end = System.currentTimeMillis();
        provenanceWriteTimeStatistic.add(end - start);
        deliveryLatencyStatistic.add(end - start);
        // NOTE: Writes will be more than reality for provenance overlap > 1
        // "Wrote" 1 sink  + 1 sink ACK + P sources + P edges + P ACKs
        provenanceWritesStatistic.increase(2 + (3 * sourceTuples.length));
        // "Read" (outputted) 1 sink + 1 sink ACK  + P sources + P edges
        provenanceReadsStatistic.increase(2 + (2 * sourceTuples.length));
    }

    /**
     * Create a <b>Serializable</b> {@link Consumer} for each watermark that calls updateProvenance.
     *
     * @return The watermark consumer for this object.
     */
    public BiConsumer<Watermark, Collector<ProvenanceGraphNode>> watermarkConsumer() {
        return (Serializable & BiConsumer<Watermark, Collector<ProvenanceGraphNode>>)
                this::updateAndEmitProvenance;
    }

    public void updateAndEmitProvenance(Watermark currentWatermark,
                                        Collector<ProvenanceGraphNode> out) {
        final long start = System.currentTimeMillis();
        ProvenanceKey currentTimeKey = inactiveDataBoundary();
//        System.out.println("Inactive data boundary /  threshold: " + currentTimeKey.timestamp());
        Iterator<ProvenanceKey> it = pendingSourceAcks.iterator();
        boolean emitted = false;
        while (it.hasNext()) {
            TupleProvenanceKey key = (TupleProvenanceKey) it.next();
            if (Objects.compare(key, currentTimeKey, ProvenanceKey::compareTo) >= 0) {
//                System.out.println("BREAK!");
                break;
            }
            out.collect(ProvenanceGraphNode.newACK(key.uid));
            it.remove();
            // Read (outputted) 1 ack
            provenanceReadsStatistic.increase(1);
            emitted = true;
//            System.out.println("EMITTED!");
        }
        final long end = System.currentTimeMillis();
        if (emitted) {
            deliveryLatencyStatistic.add(end - start);
        }
        provenanceReadTimeStatistic.add(end - start);
    }

    private ProvenanceKey inactiveDataBoundary() {
        // Outputs with timestamps lower than this are ready to be processed
        return ProvenanceKey.ofTimestamp(threshold);
    }

}
