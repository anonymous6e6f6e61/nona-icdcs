package streamingRetention;

import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import java.util.function.BiFunction;

public class WatermarkConvertingStreamSink<IN> extends StreamSink<IN> {

    private final BiFunction<Watermark,String,StreamRecord<IN>> watermarkToStreamRecordConversion;
    private final String name;

    public WatermarkConvertingStreamSink(SinkFunction<IN> sinkFunction,
                                         BiFunction<Watermark, String, StreamRecord<IN>>
                                                 watermarkToStreamRecordConversion,
                                         String name) {
        super(sinkFunction);
        this.watermarkToStreamRecordConversion = watermarkToStreamRecordConversion;
        this.name = name;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        super.processElement(element);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        StreamRecord<IN> convertedWatermark = watermarkToStreamRecordConversion.apply(mark, name);
        super.processWatermark(mark);
        super.processElement(convertedWatermark);
    }

    public static <INN> void addSink(DataStream<INN> stream, String name, int parallelism, boolean disableChaining,
                                     SinkFunction<INN> sinkFunction,
                                     BiFunction<Watermark, String, StreamRecord<INN>> watermarkToStreamRecordFunction) {
        if (sinkFunction instanceof InputTypeConfigurable) {
            ((InputTypeConfigurable) sinkFunction).setInputType(stream.getType(), stream.getExecutionConfig());
        }

        StreamSink<INN> sinkOperator =
                new WatermarkConvertingStreamSink<>(sinkFunction, watermarkToStreamRecordFunction, name);
        SinkTransformation<INN> sinkTransformation = new SinkTransformation<INN>(stream.getTransformation(),
                name, sinkOperator, parallelism);
        if (disableChaining) {
            sinkTransformation.setChainingStrategy(ChainingStrategy.NEVER);
        }
        stream.getExecutionEnvironment().addOperator(sinkTransformation);
    }
}