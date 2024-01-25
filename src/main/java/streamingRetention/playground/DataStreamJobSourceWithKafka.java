package streamingRetention.playground;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import streamingRetention.KafkaOffsetSourceCreator;
import streamingRetention.usecases.linearRoad.LinearRoadInputTupleGL;
import streamingRetention.usecases.linearRoad.LinearRoadKafkaStringDeserializerGL;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJobSourceWithKafka {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		env.addSource(KafkaOffsetSourceCreator.create("flinkReadsFromThis", "localhost:9092",
						new LinearRoadKafkaStringDeserializerGL(), "testQuery", 50))
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LinearRoadInputTupleGL>() {
					@Override
					public long extractAscendingTimestamp(LinearRoadInputTupleGL t) {
						return t.getTimestamp();
					}
				})


				.process(new ProcessFunction<LinearRoadInputTupleGL, LinearRoadInputTupleGL>() {
					@Override
					public void processElement(LinearRoadInputTupleGL t, ProcessFunction<LinearRoadInputTupleGL, LinearRoadInputTupleGL>.Context context,
											   Collector<LinearRoadInputTupleGL> collector) {
						System.out.println("type:"+t.getClass()+ "ts:" + context.timestamp() + "all:" + t.toString());
						collector.collect(t);
					}
				})

				.print();


		env.execute("Kafka Offset Test");
	}
}
