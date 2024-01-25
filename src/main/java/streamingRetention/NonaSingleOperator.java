package streamingRetention;

import ananke.output.ProvenanceGraphNode;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.*;

import java.time.Duration;
import java.util.*;

public class NonaSingleOperator extends RichSourceFunction<ProvenanceGraphNode> {
    private static final Logger LOG = LoggerFactory.getLogger(NonaSingleOperator.class);
    private final int MAX_BACKOFF_MILLIS = 100;
    private final Map<Long, Long> watermarkMap = new HashMap<>();
    private final Map<Long, Long> uMap = new HashMap<>();
    private final ExperimentSettings settings;
    private final SlackStrategy slackStrategy;
    private long threshold = 0;
    private long minWatermark = 0;
    private final Properties provenanceConsumerProps = new Properties();
    private final Properties requestsConsumerProps = new Properties();
    private final Properties replyProducerProps = new Properties();
    private final SimpleStringHashFunction hasher = new SimpleStringHashFunction();
    private long sinkTupleCounter = 0;
    public static final String EXTRACTOR_STAT_NAME = "NONA";
    private final ExecutionConfig ec;
    private final TimestampConverter timestampConverter;
    private NavigableSet<ProvenanceKey> pendingSourceAcks;
    private transient AvgStat provenanceWriteTimeStatistic;
    private transient AvgStat provenanceReadTimeStatistic;
    private transient CountStat provenanceReadsStatistic;
    private transient CountStat provenanceWritesStatistic;
    private transient MaxStat deliveryLatencyStatistic;
    private transient TimestampedStringLogger transitionTimeLogger;
    private transient TimestampedStringLogger watermarkLogger;
    private transient TimestampedStringLogger thresholdLogger;
    private transient Backoff backoff;
    private boolean isRunning;

    public NonaSingleOperator(ExperimentSettings settings, ExecutionConfig ec, TimestampConverter timestampConverter) {
        super();
        this.settings = settings;
        this.slackStrategy = settings.slackStrategy();
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
        this.transitionTimeLogger = new TimestampedStringLogger(
                settings.timeLoggerFile(indexOfThisSubtask, EXTRACTOR_STAT_NAME),
                settings.autoFlush(),
                TimestampedStringLogger.TimeUnit.MILLISECONDS);
        if (settings.logWatermarks()) {
            this.watermarkLogger = new TimestampedStringLogger(
                    settings.watermarkLoggerFile(indexOfThisSubtask, EXTRACTOR_STAT_NAME),
                    settings.autoFlush(),
                    TimestampedStringLogger.TimeUnit.SECONDS);
        }
        if (settings.logThreshold()) {
            this.thresholdLogger = new TimestampedStringLogger(
                    settings.thresholdLoggerFile(indexOfThisSubtask, EXTRACTOR_STAT_NAME),
                    settings.autoFlush(),
                    TimestampedStringLogger.TimeUnit.SECONDS);
        }
        this.pendingSourceAcks = new TreeSet<>();
        provenanceConsumerProps.put("bootstrap.servers", settings.kafkaSinkBootstrapServer());
        provenanceConsumerProps.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        provenanceConsumerProps.put("group.id", "PC-prov");
        provenanceConsumerProps.put("client.id", "consumer-prov-PC");
        requestsConsumerProps.put("bootstrap.servers", settings.kafkaSinkBootstrapServer());
        requestsConsumerProps.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        requestsConsumerProps.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        requestsConsumerProps.put("group.id", "PC-reqs");
        requestsConsumerProps.put("client.id", "consumer-reqs-PC");
        replyProducerProps.put("bootstrap.servers", settings.kafkaSinkBootstrapServer());
        replyProducerProps.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        replyProducerProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        replyProducerProps.put("client.id", "producer-reps-PC");
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
                                     0, settings.kafkaSinkTopic(), ec, ArrayUnfoldedGenealogTuple.class));
             KafkaConsumer<String, String> requestsConsumer= new KafkaConsumer<>(requestsConsumerProps);
             KafkaProducer<String, String> repliesProducer= new KafkaProducer<>(replyProducerProps) ) {
            provenanceConsumer.subscribe(Collections.singletonList(settings.kafkaSinkTopic()));
            requestsConsumer.subscribe(Collections.singletonList(settings.kafkaRequestsTopic()));
            while (isRunning) {
                ConsumerRecords<String, ArrayUnfoldedGenealogTuple> readProvRecords =
                        provenanceConsumer.poll(Duration.ofMillis(0));
                if (!readProvRecords.isEmpty()) {
                    backoff.reset();
                    for (ConsumerRecord<String, ArrayUnfoldedGenealogTuple> rec : readProvRecords) {
                        long sinkUID = rec.value().getSinkTuple().getUID();
                        switch (rec.value().getType()) {
                            case BPROVENANCE:
                                if (! isQueryInWatermarkMap(sinkUID)) continue;
                                onTuple(ctx, rec.value());
                                break;
                            case WATERMARK:
                                if (! isQueryInWatermarkMap(sinkUID)) continue;
                                onWatermark(ctx, rec.value().getSinkTuple().getTimestamp(), sinkUID);
                                break;
                            case MARKER:
                                onMarker(repliesProducer, rec.key());
                                break;
                            default:
                                throw new RuntimeException("Encountered unexpected type: " + rec.value().getType());
                        }
                    }
                } else {
                    backoff.backoff();
                }
                ConsumerRecords<String, String> readReqRecords = requestsConsumer.poll(Duration.ofMillis(0));
                if (!readReqRecords.isEmpty()) {
                    LOG.info("Received new request(s):");
                    for (ConsumerRecord<String, String> rec : readReqRecords) {
                        String[] recordValueArr = rec.value().split(",");
                        String queryID = recordValueArr[0];
                        String requestType = recordValueArr[1];
                        long queryUMilliseconds = Long.parseLong(recordValueArr[2]);
                        LOG.info("query ID: " + queryID);
                        LOG.info("hash of this query: " + hasher.apply(queryID));
                        long queryHash = hasher.apply(queryID);
                        if (Objects.equals(requestType, "add")) {
                            onAdditionRequest(repliesProducer, queryID, queryUMilliseconds, queryHash);
                        } else if (Objects.equals(requestType, "init")) {
                            onInit(repliesProducer);
                        } else {
                            throw new RuntimeException("Unknown requestType: " + requestType);
                        }
                    }
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
        updateMinWatermark();
        logWatermark();
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

    private void onAdditionRequest(KafkaProducer<String, String> repliesProducer, String queryID, long queryUMilliseconds, long queryHash) {
        LOG.info("Type of request: Issuing request");
        if (watermarkMap.containsKey(queryHash)) {
            LOG.info("Denied!");
            repliesProducer.send(new ProducerRecord<>(
                    settings.kafkaRepliesTopic(), "[duplicate query issued!],denied"));
        } else {
            long sourceLowerBoundMilliseconds;
            uMap.put(queryHash, queryUMilliseconds);
            if (watermarkMap.isEmpty()) {
                sourceLowerBoundMilliseconds = 0;
            } else {
                switch (slackStrategy) {
                    case T:
                        sourceLowerBoundMilliseconds = minWatermark;
                        break;
                    case TW:
                        sourceLowerBoundMilliseconds = threshold;
                        break;
                    default:
                        sourceLowerBoundMilliseconds = Math.max(minWatermark,
                                threshold + queryUMilliseconds);
                }
            }
            watermarkMap.put(queryHash, sourceLowerBoundMilliseconds);
            String replyString = queryID + ",accepted," + sourceLowerBoundMilliseconds;
            LOG.info("Accepted! Sent reply: " + replyString);
            repliesProducer.send(new ProducerRecord<>(
                    settings.kafkaRepliesTopic(), replyString));
        }
    }

    private void onMarker(KafkaProducer<String, String> repliesProducer, String queryID) {
        LOG.info("Received marker for query " + queryID);
        long queryHash = hasher.apply(queryID);
        String replyString = "";
        if ((!watermarkMap.containsKey(queryHash)) || (!uMap.containsKey(queryHash))) {
            replyString = queryID + ",[not found among currently deployed queries]";
            LOG.error("Denied! Query " + queryID + " with hash " + queryHash + " not found among currently" +
                    " deployed queries");
            repliesProducer.send(new ProducerRecord<>(
                    settings.kafkaRepliesTopic(), replyString));
        } else {
            uMap.remove(queryHash);
            watermarkMap.remove(queryHash);
            updateThreshold();
            if (!watermarkMap.isEmpty()) {
                updateMinWatermark();
            }
            transitionTimeLogger.log("removal-end");
            replyString = queryID + ",removed";
            LOG.info("Query forgotten!");
            repliesProducer.send(new ProducerRecord<>(
                    settings.kafkaRepliesTopic(), replyString));
            LOG.info("Sent reply: " + replyString);
        }
    }

    private void onInit(KafkaProducer<String, String> repliesProducer) {
        LOG.info("Received init message.");
        String replyString = "init,received";
        repliesProducer.send(new ProducerRecord<>(
                settings.kafkaRepliesTopic(), replyString));
        LOG.info("Sent confirmation to init: " + replyString);
    }

    @Override
    public void cancel() {
        isRunning = false;
        provenanceReadTimeStatistic.close();
        provenanceWriteTimeStatistic.close();
        provenanceReadsStatistic.close();
        provenanceWritesStatistic.close();
        transitionTimeLogger.close();
        watermarkLogger.close();
        thresholdLogger.close();
    }

    private void updateMinWatermark() {
        minWatermark = Collections.min(watermarkMap.values());
        logWatermark();
    }

    private void updateThreshold() {
        long min = Long.MAX_VALUE;
        if (!watermarkMap.isEmpty()) {
            for (long id : watermarkMap.keySet()) {
                long diff = watermarkMap.get(id) - uMap.get(id);
                min = Math.min(min, diff);
            }
        }
        // enforce threshold to be non-decreasing
        threshold = Math.max(min,threshold);
        logThreshold();
    }

    private long generateSinkTupleUID() {
        return sinkTupleCounter++;
    }

    private boolean isQueryInWatermarkMap(long sinkUID) {
        if ( ! watermarkMap.containsKey(sinkUID) ) {
            LOG.warn("Received tuple from unknown query with queryHash " + sinkUID);
            return false;
        } else {
            return true;
        }
    }

    private void logWatermark() {
        if (settings.logWatermarks()) {
            watermarkLogger.log(String.valueOf(minWatermark));
        }
    }

    private void logThreshold() {
        if (settings.logThreshold()) {
            thresholdLogger.log(String.valueOf(threshold));
        }
    }

}
