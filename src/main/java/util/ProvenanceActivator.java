package util;

import ananke.output.ProvenanceGraphExtractor;
import ananke.output.ProvenanceGraphNode;
import ananke.output.ProvenanceGraphSink;
import ananke.util.WatermarkTriggeringFlatMap;
import genealog.GenealogFileSink;
import genealog.GenealogGraphTraverser;
import genealog.GenealogTuple;
import genealog.WatermarkTriggeringSink;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import streamingRetention.*;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

public enum ProvenanceActivator {
  GENEALOG {
    @Override
    public <T extends GenealogTuple> void activate(
            List<DataStream<T>> sinkStreams,
            List<String> names,
            ExperimentSettings settings,
            long maxDelayMillis,
            TimestampConverter timestampConverter,
            ExecutionConfig ec) {
      Validate.isTrue(sinkStreams.size() == names.size(), "streams.size() != names.size()");
      for (int i = 0; i < sinkStreams.size(); i++) {
        DataStream<T> stream = sinkStreams.get(i);
        String name = names.get(i);
        stream
            .addSink(GenealogFileSink.newInstance(name, settings))
            .setParallelism(1)
            .name(name);
      }
    }

    @Override
    public <T extends UIDTuple> MapFunction<T, T> uidAssigner(
        int componentIndex, int maxInstances) {
      return (MapFunction<T, T>) value -> value;
    }
  },
  GENEALOG_UNFOLDED_TO_KAFKA {
    @Override
    public <T extends GenealogTuple> void activate(
        List<DataStream<T>> sinkStreams,
        List<String> names,
        ExperimentSettings settings,
        long maxDelayMillis,
        TimestampConverter timestampConverter,
        ExecutionConfig ec) {
      Validate.isTrue(sinkStreams.size() == 1, "Only one sink stream allowed with GENEALOG_UNFOLDED_TO_KAFKA");
      Validate.isTrue(names.size() == 1, "Only one name allowed with GENEALOG_UNFOLDED_TO_KAFKA");
      GenealogGraphTraverser genealogGraphTraverser =
              new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
      SimpleStringHashFunction hasher = new SimpleStringHashFunction();

      for (int i=0; i<sinkStreams.size(); i++) {
        DataStream<T> stream = sinkStreams.get(i);
        int finalI = i;
        DataStream<UnfoldedGenealogTuple> unfoldedProvenanceStream = stream
                .map(new LatencyLogMap<>(settings))
                .map(new MapFunction<T, UnfoldedGenealogTuple>() {
                  // get the provenance from the sink tuple t, create an array with t and the contributing source tuples
                  @Override
                  public UnfoldedGenealogTuple map(T sinkTuple) {
                    Collection <TimestampedUIDTuple> sourceTuplesSet = genealogGraphTraverser.getProvenance(sinkTuple);
                    TimestampedUIDTuple[] sourceTuples = sourceTuplesSet.toArray(new TimestampedUIDTuple[0]);
                    sinkTuple.setUID(hasher.apply(names.get(finalI)));
                    sinkTuple.stopSerializingProvenance();
                    return new ArrayUnfoldedGenealogTuple(
                            sinkTuple,
                            sourceTuples);
                  }
                })
                .setParallelism(1)
                .name("ProvenanceUnfoldingMap");
        Properties kafkaProducerConfig = new Properties();
        kafkaProducerConfig.setProperty("bootstrap.servers", settings.kafkaSinkBootstrapServer());
        kafkaProducerConfig.setProperty("client.id", "producer-" + settings.queryID());
        KafkaSerializationSchema<ArrayUnfoldedGenealogTuple> kafkaSerializationSchema =
                new KafkaKryoRecordSerializationSchema<>(0, settings.kafkaSinkTopic(), ec, ArrayUnfoldedGenealogTuple.class);
        FlinkKafkaProducer flinkKafkaProducer = new FlinkKafkaProducer<>(
                settings.kafkaSinkTopic(),
                kafkaSerializationSchema,
                kafkaProducerConfig,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        WatermarkConvertingStreamSink.addSink(
                unfoldedProvenanceStream,
                names.get(i),
                1,
                settings.disableSinkChaining(),
                flinkKafkaProducer,
                new SimpleNamedWatermarkConversionFunction(hasher));

      }
    }
    @Override
    public <T extends UIDTuple> MapFunction<T, T> uidAssigner(
            int componentIndex, int maxInstances) {
      return t -> {
        t.setUID(t.toString().hashCode());
        return t;
      };
    }
  },
  ANANKE {
    @Override
    public <T extends GenealogTuple> void activate(
        List<DataStream<T>> sinkStreams,
        List<String> names,
        ExperimentSettings settings,
        long maxDelayMillis,
        TimestampConverter timestampConverter,
        ExecutionConfig ec) {

      Validate.isTrue(sinkStreams.size() == names.size(), "streams.size() != names.size()");
      ProvenanceGraphExtractor<GenealogTuple> graphExtractor =
          new ProvenanceGraphExtractor<>(settings, maxDelayMillis, timestampConverter);
      DataStream<GenealogTuple> mergedSinkStream =
          getMergedSinkStream(sinkStreams, names, settings);
      DataStream<ProvenanceGraphNode> graphNodeDataStream = WatermarkTriggeringFlatMap.connect(
          mergedSinkStream, graphExtractor,
          graphExtractor.watermarkConsumer()
      ).setParallelism(1).name(name());
      DataStreamSink<ProvenanceGraphNode> streamSink = graphNodeDataStream
          .addSink(new ProvenanceGraphSink("SINK", settings, timestampConverter))
          .setParallelism(1)
          .slotSharingGroup(settings.thirdSlotSharingGroup());
      if (settings.disableSinkChaining()) {
        streamSink.disableChaining();
      }
    }

    @Override
    public <T extends UIDTuple> MapFunction<T, T> uidAssigner(
        int componentIndex, int maxInstances) {
      return new UIDAssigner<>(componentIndex, maxInstances);
    }
  };

  public static <T extends GenealogTuple> DataStream<GenealogTuple> getMergedSinkStream(
      List<DataStream<T>> sinkStreams, List<String> names, ExperimentSettings settings) {
    List<DataStream<GenealogTuple>> extendedStreams = new ArrayList<>();
    for (int i = 0; i < sinkStreams.size(); i++) {
      int componentIndex = (settings.sourcesNumber() + i) * settings.maxParallelism();
      DataStream<T> sinkStream = sinkStreams.get(i);
      DataStream<GenealogTuple> extendedStream =
          sinkStream
              .map(new LatencyLoggingMap<T, GenealogTuple>(names.get(i), settings))
              .setParallelism(sinkStream.getParallelism())
              .name("LatencyLogger")
              .returns(GenealogTuple.class)
              .map(new UIDAssigner<>(componentIndex, settings.maxParallelism()))
              .setParallelism(sinkStream.getParallelism())
              .name("UIDAssigner");
      extendedStreams.add(extendedStream);
    }
    DataStream<GenealogTuple> union = extendedStreams.get(0);
    if (extendedStreams.size() > 1) {
      union =
          union.union(
              extendedStreams.subList(1, extendedStreams.size()).toArray(new DataStream[0]));
    }
    return union;
  }

  public abstract <T extends GenealogTuple> void activate(
      List<DataStream<T>> sinkStreams,
      List<String> names,
      ExperimentSettings settings,
      long maxDelayMillis,
      TimestampConverter timestampConverter,
      ExecutionConfig ec);

  public static <T extends GenealogTuple> DataStream<GenealogTuple> convert(
      DataStream<T> sinkStream) {
    return (DataStream<GenealogTuple>) sinkStream;
  }

  public abstract <T extends UIDTuple> MapFunction<T, T> uidAssigner(
      int componentIndex, int maxInstances);
}
