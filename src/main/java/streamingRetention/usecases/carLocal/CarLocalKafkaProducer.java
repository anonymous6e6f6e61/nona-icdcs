package streamingRetention.usecases.carLocal;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

public class CarLocalKafkaProducer {

    static long maxNumTuples = 800000;
    static double reportGranularity = 0.10;

    static long get_ts(File file) {
        String[] fileNameWithoutEndingSplitByUnderscore =  file.getName().replace("objects_","").split("\\.")[0].split("-");
        return Long.parseLong(fileNameWithoutEndingSplitByUnderscore[fileNameWithoutEndingSplitByUnderscore.length-1]);
    }

    public static byte[] convertToByteArray(Object object) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(object);
        oos.flush();
        byte[] bytes = bos.toByteArray();
        oos.close();
        bos.close();
        return bytes;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        System.out.println("Starting");
        assert args.length == 3 : "Wrong number of arguments, expected 3";
        String folderpath = args[0];
        String bootstrapServer = args[1];
        String sourceTopic = args[2];

        List<T4<Long, String, File, File>> timestampList= new ArrayList<>();

        File folder = new File(folderpath);
        File[] listOfFiles = folder.listFiles();

//        File orderedFilelist = new File(folderpath + "/" + "orderedFilelist");
//
//        boolean listExists = orderedFilelist.exists();
//
        List<T7<Long, File, File, File, File, File, File>> combinedTimestampList;
//
//        // we now need to load all filenames, match image/lidar file with its annotation file and sort everything
//        // this is costly, so let's save the resulting ordered list. Before doing this, let's check if we already have
//        // such a list (in the location given by 'folderpath'), and if we have, let's load that.
//        // Then, we do not need to do this step every time we run the program. Nice, isn't it?
//        if (listExists) {
//            System.out.println("Ordered filelist exists already, loading it now: " + orderedFilelist.getAbsolutePath());
//            try {
//                FileInputStream fileIn = new FileInputStream(folderpath + "/" + "orderedFilelist");
//                ObjectInputStream objectIn = new ObjectInputStream(fileIn);
//                combinedTimestampList = (List<T7<Long, File, File, File, File, File, File>>) objectIn.readObject();
//                System.out.println("Load succesful");
//            } catch (Exception ex) {
//                ex.printStackTrace();
//            }
//
//        } else {
//
//            System.out.println("No existing ordered filelist found, creating it now, Reading files into list and sorting by timestamp");

            for (File file : listOfFiles) {
                if (file.getName().contains("txt")) {
                    String cameraType;
                    cameraType = file.getName().replace("objects_","").split("-")[0]; //this is lidar, or ring_front_center, or ...
                    timestampList.add(new T4<>(get_ts(file), cameraType, file, file));
                }
            }

            // sort the list of timestamps and files by timestamps
            timestampList.sort(Comparator.comparing(t0 -> t0.f0));

            combinedTimestampList = new ArrayList<>();

            for (int i = 0; i < timestampList.size(); i += 3) {
                T4<Long, String, File, File> elem_1 = timestampList.get(i);
                T4<Long, String, File, File> elem_2 = timestampList.get(i + 1);
                T4<Long, String, File, File> elem_3 = timestampList.get(i + 2);

                T4<Long, String, File, File> elem1 = new T4<>();
                T4<Long, String, File, File> elem2 = new T4<>();
                T4<Long, String, File, File> elem3 = new T4<>();

                if (elem_1.f1.equals("lidar")) {
                    elem1 = elem_1;
                } else if (elem_1.f1.contains("left")) {
                    elem2 = elem_1;
                } else {
                    elem3 = elem_1;
                }
                if (elem_2.f1.equals("lidar")) {
                    elem1 = elem_2;
                } else if (elem_2.f1.contains("left")) {
                    elem2 = elem_2;
                } else {
                    elem3 = elem_2;
                }
                if (elem_3.f1.equals("lidar")) {
                    elem1 = elem_3;
                } else if (elem_3.f1.contains("left")) {
                    elem2 = elem_3;
                } else {
                    elem3 = elem_3;
                }
                combinedTimestampList.add(T7.of(elem1.f0, elem1.f2, elem1.f3, elem2.f2, elem2.f3, elem3.f2, elem3.f3));
            }

//            System.out.println("Writing ordered filelist to disk for future use");
//
//            try {
//                FileOutputStream fileOut = new FileOutputStream(folderpath + "/" + "orderedFilelist");
//                ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
//                objectOut.writeObject(combinedTimestampList);
//                objectOut.close();
//                System.out.println("Ordered filelist written to disk");
//            } catch (Exception ex) {
//                System.out.println("Encountered an error:");
//                ex.printStackTrace();
//                return;
//            }
//        }

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
        long stepSize = (long) (maxNumTuples * reportGranularity);
        // emit LidarImageContainer objects
        try( Producer<String, byte[]> producer = new KafkaProducer<>(props) ) {
            System.out.println("Connected to Kafka");
            int originalTupleIndex = 0;
            long tuplesProducedCount = 0;
            long maxTS = -1;
            assert combinedTimestampList != null : "Combined timestamp list is not null!";
            int lengthList = combinedTimestampList.size();
            long highestTSinList = combinedTimestampList.get(lengthList - 1).f0;
            long lowestTSinList = combinedTimestampList.get(0).f0;

            while (tuplesProducedCount < maxNumTuples) {
                T7<Long, File, File, File, File, File, File> timestampFilesAnnotations =
                        combinedTimestampList.get(originalTupleIndex);
                long timestampNanoseconds = (long) timestampFilesAnnotations.f0
                        + tuplesProducedCount / lengthList * (highestTSinList - lowestTSinList + 1);
                long timestampMilliseconds = timestampNanoseconds / 1000000;
                LidarImageContainer container1 = new LidarImageContainer("lidar", timestampMilliseconds);
                container1.createAnnotations((File) timestampFilesAnnotations.f2);
                LidarImageContainer container2 = new LidarImageContainer("ring_front_left", timestampMilliseconds);
                container2.createAnnotations((File) timestampFilesAnnotations.f4);
                LidarImageContainer container3 = new LidarImageContainer("ring_front_right", timestampMilliseconds);
                container3.createAnnotations((File) timestampFilesAnnotations.f6);

                try {
                    byte[] payload = convertToByteArray(
                            new LidarImageContainer[]{container1,container2,container3});
                    ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                            sourceTopic, partition, timestampMilliseconds, "noKey", payload);
                    producer.send(record);
                    tuplesProducedCount++;
                    maxTS = Math.max(maxTS, timestampMilliseconds);
                } catch (Exception e) {
                    System.out.println("Failed to send record to Kafka: " + e);
                }

                if ( tuplesProducedCount % stepSize == 0 ) {
                    System.out.println((int) (((double) tuplesProducedCount) / maxNumTuples * 100) + "% processed /  "
                            + tuplesProducedCount + " tuples");
                }

                originalTupleIndex++;
                originalTupleIndex = originalTupleIndex % lengthList;
            }
            System.out.println("Done feeding to Kafka at " + bootstrapServer + " and topic " + sourceTopic);
            System.out.println("Produced " + tuplesProducedCount + " tuples, maximum timestamp " + maxTS);
        }
    }
}
