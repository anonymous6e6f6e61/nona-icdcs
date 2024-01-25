package streamingRetention;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ExperimentSettings;
import util.SlackStrategy;
import util.TimestampConverter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ProvenanceCombiner implements SourceFunction<AnnotatedArrayUnfoldedGenealogTuple> {

    private static final Logger LOG = LoggerFactory.getLogger(ProvenanceCombiner.class);
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
    private boolean thresholdUpdated = false;
    private long sinkTupleCounter = 0;
    private final ExecutionConfig ec;
    private boolean isRunning;

    private final TimestampConverter timestampConverter;

    public ProvenanceCombiner(ExperimentSettings settings, ExecutionConfig ec, TimestampConverter timestampConverter) {
        this.settings = settings;
        this.slackStrategy = settings.slackStrategy();
        this.ec = ec;
        this.timestampConverter = timestampConverter;
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
        isRunning = true;
    }

    @Override
    public void run(SourceContext<AnnotatedArrayUnfoldedGenealogTuple> ctx) throws Exception {
        throw new Exception("Class is deprecated!");
//        try (KafkaConsumer<String, ArrayUnfoldedGenealogTuple> provenanceConsumer =
//                     new KafkaConsumer<>(
//                             provenanceConsumerProps,
//                             new StringDeserializer(),
//                             new KafkaKryoRecordSerializationSchema<>(
//                                     0, settings.kafkaSinkTopic(), ec, ArrayUnfoldedGenealogTuple.class));
//             KafkaConsumer<String, String> requestsConsumer= new KafkaConsumer<>(requestsConsumerProps);
//             KafkaProducer<String, String> repliesProducer= new KafkaProducer<>(replyProducerProps) ) {
//            provenanceConsumer.subscribe(Collections.singletonList(settings.kafkaSinkTopic()));
//            requestsConsumer.subscribe(Collections.singletonList(settings.kafkaRequestsTopic()));
//            while (isRunning) {
//
//                // check the provenance topic for new data
//                // pull provenance data from Kafka
//
//                // 1) check if there is new data in the provenance channel
//                // 2) find out if it is provenance tuples or a watermark, and find out the sending query
//                // 2.1) if it is a watermark, update the value in watermarkMap AND the value of minWatermark AND threshold
//                // 2.2) if it is provenance tuples, produceProvenanceTuples()
//
//                // @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
//                // @ #1 PRODUCE BPROVENANCE AND WATERMARKS @
//                // @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
//                ConsumerRecords<String, ArrayUnfoldedGenealogTuple> readProvRecords = provenanceConsumer.poll(Duration.ofMillis(0));
//                if (!readProvRecords.isEmpty()) {
//                    for (ConsumerRecord<String, ArrayUnfoldedGenealogTuple> rec : readProvRecords) {
//                        long sinkUID = rec.value().getSinkTuple().getUID();
////                        if (!watermarkMap.containsKey(sinkUID)) {
////                            continue; // if this tuple comes from a non-registered sink, it is from a retired query
////                        }
//                        if (rec.value().isFromWatermark()) {
//                            onWatermark(ctx, rec.value().getSinkTuple().getTimestamp(), sinkUID);
//                        } else {
//                            onTuple(ctx, rec.value());
//                        }
//                    }
//                }
//
//                // @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
//                // @@ #2 CHECK REQUESTS CHANNEL @@
//                // @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
//                ConsumerRecords<String, String> readReqRecords = requestsConsumer.poll(Duration.ofMillis(0));
//                if (!readReqRecords.isEmpty()) {
//                    LOG.info("Received new request(s):");
//                    for (ConsumerRecord<String, String> rec : readReqRecords) {
//                        String[] recordValueArr = rec.value().split(",");
//                        String queryID = recordValueArr[0];
//                        String type = recordValueArr[1];
//                        long queryUMilliseconds = Long.parseLong(recordValueArr[2]);
//                        LOG.info("query ID: " + queryID);
//                        LOG.info("hash of this query: " + hasher.apply(queryID));
//                        Long queryHash = hasher.apply(queryID);
//
//                        if (Objects.equals(type, "issue")) {
//                            LOG.info("Type of request: Issuing request");
//                            if (watermarkMap.containsKey(queryHash)) {
//                                LOG.info("Denied!");
//                                repliesProducer.send(new ProducerRecord<>(
//                                        settings.kafkaRepliesTopic(), "[duplicate query issued!],denied"));
//                            } else {
//                                long sourceLowerBoundMilliseconds;
//                                uMap.put(queryHash, queryUMilliseconds);
//                                if (watermarkMap.isEmpty()) {
//                                    sourceLowerBoundMilliseconds = 0;
//                                } else {
//                                    switch (slackStrategy) {
//                                        case T:
//                                            sourceLowerBoundMilliseconds = minWatermark;
//                                            break;
//                                        case TW:
//                                            sourceLowerBoundMilliseconds = threshold;
//                                            break;
//                                        default:
//                                            sourceLowerBoundMilliseconds = Math.max(minWatermark,
//                                                    threshold + queryUMilliseconds);
//                                    }
//
//                                }
//                                watermarkMap.put(queryHash, sourceLowerBoundMilliseconds);
//                                String replyString = queryID + "," + sourceLowerBoundMilliseconds;
//                                LOG.info("Accepted! Sent reply: " + replyString);
//                                repliesProducer.send(new ProducerRecord<>(
//                                        settings.kafkaRepliesTopic(), replyString));
//                            }
//                        } else if (Objects.equals(type, "retire")) {
//                            LOG.info("Type of request: Retiring request");
//                            if (!watermarkMap.containsKey(queryHash)) {
//                                LOG.info("Denied! Query with hash " + queryHash + " not found among currently" +
//                                        " deployed queries");
//                                repliesProducer.send(new ProducerRecord<>(
//                                        settings.kafkaRepliesTopic(), "[query not found!],denied"));
//                            } else {
//                                uMap.remove(queryHash);
//                                watermarkMap.remove(queryHash);
//                                String replyString = queryID + "," + "-1";
//                                LOG.info("Query removed! Sent reply " + replyString);
//                                repliesProducer.send(new ProducerRecord<>(
//                                        settings.kafkaRepliesTopic(), replyString));
//                            }
//                        }
//                    }
//                }
//
//            }
//        }

    }

    public void onTuple(SourceContext<AnnotatedArrayUnfoldedGenealogTuple> ctx, ArrayUnfoldedGenealogTuple tuple) {
        // generate a new SinkUID, overwriting the old one
        tuple.getSinkTuple().setUID(generateSinkTupleUID());
        if (thresholdUpdated) {
            ctx.collectWithTimestamp(
                    AnnotatedArrayUnfoldedGenealogTuple.of(tuple, threshold),
                    timestampConverter.apply(tuple.getTimestamp()));
            thresholdUpdated = false;
        } else {
            ctx.collectWithTimestamp(
                    AnnotatedArrayUnfoldedGenealogTuple.of(tuple),
                    timestampConverter.apply(tuple.getTimestamp()));
        }
    }

    public void onWatermark(SourceContext<AnnotatedArrayUnfoldedGenealogTuple> ctx, long watermark, long sinkUID) {
        watermarkMap.put(sinkUID, watermark);
        long prevMinWatermark = minWatermark;
        minWatermark = Collections.min(watermarkMap.values());
        updateThreshold();
        thresholdUpdated = true;
        if (minWatermark > prevMinWatermark) {
            ctx.emitWatermark(new Watermark(minWatermark));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private void updateThreshold() {
        long min = Long.MAX_VALUE;
        if (!watermarkMap.isEmpty()) {
            for (long id : watermarkMap.keySet()) {
                long diff = watermarkMap.get(id) - uMap.get(id);
                min = Math.min(min, diff);
            }
        }
        threshold = Math.max(min,0);
    }

    private long generateSinkTupleUID() {
        return sinkTupleCounter++;
    }

}
