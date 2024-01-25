package streamingRetention.playground.dummy;

import genealog.GenealogTupleType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import util.ExperimentSettings;
import util.ProvenanceActivator;
import util.TimestampConverter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static streamingRetention.usecases.linearRoad.LinearRoadConstants.ACCIDENT_WINDOW_SIZE;

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
public class DummyGenealogJob {

	public static void main(String[] args) throws Exception {


		ExperimentSettings settings = ExperimentSettings.newInstance(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final TimestampConverter timestampConverter = (ts) -> ts;
		final Time WS = Time.seconds(2);
		final Time WA = Time.seconds(1);

		env.setParallelism(1);


		long lengthDummyData = 10;
		List<DummyGLTuple> dummyData = new ArrayList<>();
		for (int i=0; i<lengthDummyData; i++) {
			DummyGLTuple dummy = new DummyGLTuple((i+1)* 500L, System.currentTimeMillis(), (i+1)*2);
			dummy.initGenealog(GenealogTupleType.SOURCE);
			dummyData.add(dummy);
		}

		DataStream<DummyGLTuple> stream =
				env
						.fromCollection(dummyData)
						.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<DummyGLTuple>() {
							@Override
							public long extractAscendingTimestamp(DummyGLTuple t) {
								return t.getTimestamp();
							}
						})
						.windowAll(SlidingEventTimeWindows.of(WS, WA))
						.aggregate(new DummyGLAggregate(settings.aggregateStrategySupplier()))
//						.process(new ProcessFunction<DummyGLTuple, DummyGLTuple>() {
//							@Override
//							public void processElement(DummyGLTuple dummyGLTuple,
//													   ProcessFunction<DummyGLTuple, DummyGLTuple>.Context context,
//													   Collector<DummyGLTuple> collector) throws Exception {
//								System.out.println(dummyGLTuple);
//								collector.collect(dummyGLTuple);
//							}
//						})
						;

		settings
				.genealogActivator()
				.activate(
						Arrays.asList(ProvenanceActivator.convert(stream)),
						Arrays.asList("DummyStream"),
						settings,
						ACCIDENT_WINDOW_SIZE.toMilliseconds(),
						timestampConverter,
						env.getConfig()
				);

		env.execute("Dummy GL Job");
	}
}
