package streamingRetention.usecases.linearRoad;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class LinearRoadKafkaProducer {

    public static void main(String[] args) throws FileNotFoundException, IllegalArgumentException {

        if (args.length != 4) {
            throw new IllegalArgumentException("Expected 4 arguments: filepath, Kafka bootstrap server, source topic, " +
                    "   and numTuplesToProduce. Received " + args.length + " args!");
        }

        String filePath = args[0];
        String bootstrapServer = args[1];
        String sourceTopic = args[2];
        long numTuplesToProduce = Long.parseLong(args[3]);

        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServer);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Maximum time to wait to complete a batch
        props.put("linger.ms", 0);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("client.id", "LinearRoadKafkaProducer");

        int partition = 0;

        long numProducedTuples = 0;
        double reportGranularity = 0.1;
        long stepSize = Math.max( (long) (numTuplesToProduce * reportGranularity), 1L);
        int maxTS = Integer.MIN_VALUE;
        int minTS = Integer.MAX_VALUE;
        int maxMappedTS = Integer.MIN_VALUE;
        int numFileReadCompletely = 0;
        System.out.println("Start reading.");
        try ( Producer<String, byte[]> producer = new KafkaProducer<>(props) ) {

            while (numProducedTuples < numTuplesToProduce) {

                BufferedReader reader = new BufferedReader(new FileReader(filePath));
                String line = reader.readLine();
                while ((line != null) && (numProducedTuples < numTuplesToProduce)) {
                    String[] words = line.split(",");
                    int ts = Integer.parseInt(words[1]);
                    String vid = words[2];
                    maxTS = Math.max(maxTS, ts);
                    minTS = Math.min(minTS, ts);
                    int mappedTS = ts + numFileReadCompletely * (maxTS - minTS + 1);
                    maxMappedTS = Math.max(maxMappedTS, mappedTS);
                    byte[] lineAsBytes = LinearRoadReadingUtil.getBytesFromReading(line, mappedTS);
                    ProducerRecord<String, byte[]> record =
                            new ProducerRecord<>(sourceTopic, partition, (long) mappedTS, vid, lineAsBytes);
                    try {
                        producer.send(record);
                        numProducedTuples++;
                    } catch (Exception e) {
                        System.out.println("Failed to send record " + record + " to kafka: " + e);
                    }
                    if (numProducedTuples % stepSize == 0) {
                        System.out.println((int) (((double) numProducedTuples) / numTuplesToProduce * 100)
                                + "% produced : " + numProducedTuples + " tuples");
                    }
                    line = reader.readLine();
                }
                reader.close();
                numFileReadCompletely++;
            }

        } catch (IOException e) {
            throw new FileNotFoundException("File " + filePath + " not found.");
        }

        System.out.println("Done feeding to Kafka at " + bootstrapServer + " and topic " + sourceTopic);
        System.out.println("Produced " + numProducedTuples + " tuples, maximum timestamp " + maxMappedTS +
                            " (max legal integer: " + Integer.MAX_VALUE + ")");

    }


    public static long countLinesInFile(String filePath) throws FileNotFoundException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            return reader.lines().count();
        } catch (IOException e) {
            throw new FileNotFoundException("File " + filePath + " not found.");
        }
    }
}