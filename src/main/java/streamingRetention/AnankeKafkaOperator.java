package streamingRetention;

import ananke.output.ProvenanceGraphNode;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.*;
import java.time.Duration;
import java.util.*;

public class AnankeKafkaOperator extends RichSourceFunction<ProvenanceGraphNode> {
    private static final Logger LOG = LoggerFactory.getLogger(AnankeKafkaOperator.class);
    private final int MAX_BACKOFF_MILLIS = 100;
    private final Map<Long, Long> watermarkMap = new HashMap<>();
    private final Map<Long, Long> uMap = new HashMap<>();
    private final ExperimentSettings settings;
    private long threshold = 0;
    private long minWatermark = 0;
    private final Properties provenanceConsumerProps = new Properties();
    private final SimpleStringHashFunction hasher = new SimpleStringHashFunction();
    private long sinkTupleCounter = 0;
    public static final String EXTRACTOR_STAT_NAME = "ANKK";
    private final ExecutionConfig ec;
    private final TimestampConverter timestampConverter;
    private NavigableSet<ProvenanceKey> pendingSourceAcks;
    private transient AvgStat provenanceWriteTimeStatistic;
    private transient AvgStat provenanceReadTimeStatistic;
    private transient CountStat provenanceReadsStatistic;
    private transient CountStat provenanceWritesStatistic;
    private transient MaxStat deliveryLatencyStatistic;
    private transient Backoff backoff;
    private boolean isRunning;

    public AnankeKafkaOperator(ExperimentSettings settings, ExecutionConfig ec, TimestampConverter timestampConverter) {
        super();
        this.settings = settings;
        this.ec = ec;
        this.timestampConverter = timestampConverter;
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
        provenanceConsumerProps.put("bootstrap.servers", settings.kafkaSinkBootstrapServer());
        provenanceConsumerProps.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        provenanceConsumerProps.put("group.id", "PC-prov");
        provenanceConsumerProps.put("client.id", "consumer-prov-PC");
        initialize(settings);
        this.backoff = new Backoff(MAX_BACKOFF_MILLIS);
        isRunning = true;
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<ProvenanceGraphNode> ctx) throws Exception {

        try (KafkaConsumer<String, ArrayUnfoldedGenealogTuple> provenanceConsumer =
                     new KafkaConsumer<>(
                             provenanceConsumerProps,
                             new StringDeserializer(),
                             new KafkaKryoRecordSerializationSchema<>(
                                     0, settings.kafkaSinkTopic(), ec, ArrayUnfoldedGenealogTuple.class))) {
            provenanceConsumer.subscribe(Collections.singletonList(settings.kafkaSinkTopic()));
            while (isRunning) {
                ConsumerRecords<String, ArrayUnfoldedGenealogTuple> readProvRecords =
                        provenanceConsumer.poll(Duration.ofMillis(0));
                if (!readProvRecords.isEmpty()) {
                    backoff.reset();
                    for (ConsumerRecord<String, ArrayUnfoldedGenealogTuple> rec : readProvRecords) {
                        long sinkUID = rec.value().getSinkTuple().getUID();
                        switch (rec.value().getType()) {
                            case BPROVENANCE:
                                onTuple(ctx, rec.value());
                                break;
                            case WATERMARK:
                                onWatermark(ctx, rec.value().getSinkTuple().getTimestamp(), sinkUID);
                                break;
                            case MARKER:
                                throw new RuntimeException("Type " + rec.value().getType() + " unsupported for Ananke");
                            default:
                                throw new RuntimeException("Encountered unexpected type: " + rec.value().getType());
                        }
                    }
                } else {
                    backoff.backoff();
                }
            }
        }
    }

    private void onTuple(SourceContext<ProvenanceGraphNode> ctx, ArrayUnfoldedGenealogTuple tuple) {
        // generate a new SinkUID, overwriting the old one
        tuple.getSinkTuple().setUID(generateSinkTupleUID());
        final long start = System.currentTimeMillis();
        TimestampedUIDTuple sinkTuple = tuple.getSinkTuple();
        TimestampedUIDTuple[] sourceTuples = tuple.getSourceTuples();
        ctx.collectWithTimestamp(ProvenanceGraphNode.newSink(sinkTuple), minWatermark);
        for (TimestampedUIDTuple sourceTuple : sourceTuples) {
            ProvenanceKey sourceTupleKey = ProvenanceKey.ofTuple(sourceTuple, timestampConverter, start);
            if (!pendingSourceAcks.contains(sourceTupleKey)) {
                ctx.collectWithTimestamp(ProvenanceGraphNode.newSource(sourceTuple), minWatermark);
                pendingSourceAcks.add(sourceTupleKey);
            }
            ctx.collectWithTimestamp(
                    ProvenanceGraphNode.newEdge(sourceTuple.getUID(), sinkTuple.getUID()),
                    minWatermark);
        }
        ctx.collectWithTimestamp(ProvenanceGraphNode.newACK(sinkTuple.getUID()), minWatermark);
        final long end = System.currentTimeMillis();
        provenanceWriteTimeStatistic.add(end - start);
        deliveryLatencyStatistic.add(end - start);
        // NOTE: Writes will be more than reality for provenance overlap > 1
        // "Wrote" 1 sink  + 1 sink ACK + P sources + P edges + P ACKs
        provenanceWritesStatistic.increase(2 + (3L * sourceTuples.length));
        // "Read" (outputted) 1 sink + 1 sink ACK  + P sources + P edges
        provenanceReadsStatistic.increase(2 + (2L * sourceTuples.length));
    }

    private void onWatermark(SourceContext<ProvenanceGraphNode> ctx, long watermark, long sinkUID) {
        watermarkMap.put(sinkUID, watermark);
        long prevMinWatermark = minWatermark;
        minWatermark = Collections.min(watermarkMap.values());
        updateThreshold();
        if (minWatermark > prevMinWatermark) {
            final long start = System.currentTimeMillis();
            ProvenanceKey currentTimeKey = ProvenanceKey.ofTimestamp(threshold);
            Iterator<ProvenanceKey> pendingSourceAcksIterator = pendingSourceAcks.iterator();
            boolean emitted = false;
            while (pendingSourceAcksIterator.hasNext()) {
                TupleProvenanceKey key = (TupleProvenanceKey) pendingSourceAcksIterator.next();
                if (Objects.compare(key, currentTimeKey, ProvenanceKey::compareTo) >= 0) {
                    break;
                }
                ctx.collectWithTimestamp(ProvenanceGraphNode.newACK(key.uid), minWatermark);
                pendingSourceAcksIterator.remove();
                provenanceReadsStatistic.increase(1);
                emitted = true;
            }
            final long end = System.currentTimeMillis();
            if (emitted) {
                deliveryLatencyStatistic.add(end - start);
            }
            provenanceReadTimeStatistic.add(end - start);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        provenanceReadTimeStatistic.close();
        provenanceWriteTimeStatistic.close();
        provenanceReadsStatistic.close();
        provenanceWritesStatistic.close();
    }

    private void updateThreshold() {
        long thresholdCandidate = 0;
        if (!watermarkMap.isEmpty()) {
            thresholdCandidate = minWatermark - Collections.max(uMap.values());
        }
        threshold = Math.max(thresholdCandidate,0);
    }

    private void initialize(ExperimentSettings settings) {
        Map<String, String> preregisteredQueries = settings.preregisteredQueries();
        for (String queryID : preregisteredQueries.keySet()) {
            watermarkMap.put(hasher.apply(queryID), Long.MIN_VALUE);
            uMap.put(hasher.apply(queryID), Long.parseLong(preregisteredQueries.get(queryID)));
        }
    }

    private long generateSinkTupleUID() {
        return sinkTupleCounter++;
    }

}
