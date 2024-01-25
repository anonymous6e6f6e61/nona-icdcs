package streamingRetention.playground;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import streamingRetention.ProvenanceCombiner;
import streamingRetention.SecToMillisConverter;
import util.ExperimentSettings;
import util.TimestampConverter;

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
public class ProvenanceCombinerTester {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		TimestampConverter timestampConverter = new SecToMillisConverter();
		ExperimentSettings settings = ExperimentSettings.newInstance(args);
		System.out.println(settings);

		env.addSource(new ProvenanceCombiner(settings, env.getConfig(), timestampConverter))
						.print();

								env.execute("Provenance Combiner Test");
	}
}
