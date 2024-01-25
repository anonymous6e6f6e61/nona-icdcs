package streamingRetention.usecases.riot;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

public class MhealthKafkaProducer {

    public static void main(String[] args) throws FileNotFoundException, IllegalArgumentException {

        if (args.length != 4) {
            throw new IllegalArgumentException("Expected 4 arguments: folderpath, Kafka bootstrap server, source topic, " +
                    "   and numTuplesToProduce. Received " + args.length + " args!");
        }

        String folderPath = args[0];
        File dir = new File(folderPath);

        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("Provided path is not a folder!");
        }

        File[] logFiles = dir.listFiles((d,name) -> name.endsWith(".log"));

        String bootstrapServer = args[1];
        String sourceTopic = args[2];
        long numTuplesToProduce = Long.parseLong(args[3]);

        Properties props = new Properties();
        Pattern DELIMITER_PATTERN = Pattern.compile("\t");

        props.put("bootstrap.servers", bootstrapServer);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Maximum time to wait to complete a batch
        props.put("linger.ms", 0);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("client.id", "LinearRoadKafkaProducer");

        int partition = 0;

        long numProducedTuples = 0;
        double reportGranularity = 0.1;
        long reportingStepSize = Math.max( (long) (numTuplesToProduce * reportGranularity), 1L);
        int ts = 0;
        int tsStepSize = 20;
        int numFileReadCompletely = 0;
        System.out.println("Start reading.");
        try ( Producer<String, String> producer = new KafkaProducer<>(props) ) {

            while (numProducedTuples < numTuplesToProduce) {

                BufferedReader[] readers = new BufferedReader[logFiles.length];
                for (int i=0; i<logFiles.length; i++) {
                    readers[i] = new BufferedReader(new FileReader(logFiles[i]));
                }

                String[] nextLines = new String[logFiles.length];
                for (int i=0; i<logFiles.length; i++) {
                    nextLines[i] = readers[i].readLine();
                }

                while ( (!Arrays.stream(nextLines).allMatch(Objects::isNull)) &&
                        (numProducedTuples < numTuplesToProduce) ) {

                    for (String line : nextLines) {
                        if (line != null) {
                            String[] tokens = DELIMITER_PATTERN.split(line.trim());
                            String accelerationChestX = tokens[0];
                            String accelerationChestY = tokens[1];
                            String accelerationChestZ = tokens[2];
                            String ecgSignalLead1 = tokens[3];
                            String ecgSignalLead2 = tokens[4];
                            String activityLabel = tokens[23];
                            String amendedLine = ts + "," + accelerationChestX + "," + accelerationChestY  + ","
                                    + accelerationChestZ  + "," + ecgSignalLead1  + "," + ecgSignalLead2  + ","
                                    + activityLabel;
                            ProducerRecord<String, String> record =
                                    new ProducerRecord<>(sourceTopic, partition, (long) ts, activityLabel, amendedLine);
                            try {
                                producer.send(record);
                                numProducedTuples++;
                            } catch (Exception e) {
                                System.out.println("Failed to send record " + record + " to kafka: " + e);
                            }
                            if (numProducedTuples % reportingStepSize == 0) {
                                System.out.println((int) (((double) numProducedTuples) / numTuplesToProduce * 100)
                                        + "% produced : " + numProducedTuples + " tuples");
                            }
                        }
                    }
                    ts += tsStepSize;
                    for (int i=0; i<logFiles.length; i++) {
                        nextLines[i] = readers[i].readLine();
                    }
                }
                for (BufferedReader reader : readers) {
                    reader.close();
                }
                numFileReadCompletely++;
            }

        } catch (java.io.IOException e) {
            throw new RuntimeException();
        }

        System.out.println("Done feeding to Kafka at " + bootstrapServer + " and topic " + sourceTopic);
        System.out.println("Produced " + numProducedTuples + " tuples in " + numFileReadCompletely + " file runs, " +
                "maximum timestamp " + ts + " (max legal integer: " + Integer.MAX_VALUE + ")");
    }

}
