package de.tuberlin.io;

import org.ini4j.Wini;

import java.io.File;
import java.io.IOException;

/**
 * Created by Lehmann on 01.12.2016.
 */
public class Conf {

 int windowType;  // tumbling|sliding|count
 int windowSize;  // measured in seconds
 int windowSlideSize;
 int countSize;
 int servingSpeedFactor;// 600 = events of 10 minutes are served in 1 second
 int maxEventDelay; // 60 =  events are out of order by max 60 seconds
 String localZookeeperHost;
 String localKafkaBroker;
 String groupId;
 String topicName;
 String master;
 int fromKafka;
 int flink;
 int batchsize;
 int maxReceiverRate;
 int kafkaProducer;
 int newOffset;
 int workload;
 int numberRecords;
 String filepath;
 int produceLoop;
 int timeout;
 int writeOutput;
 String confPath="src/main/resources/config.ini";
 String outputPath;
 int clusterMode;
    public int getWindowType() {
        return windowType;
    }


    public int getWindowSize() {
        return windowSize;
    }


    public int getCountSize() {return countSize;}

    public int getWindowSlideSize() {
        return windowSlideSize;
    }

    public int getServingSpeedFactor() {return servingSpeedFactor;}

    public int getMaxEventDelay() {return maxEventDelay;}

    public String getLocalZookeeperHost() {return localZookeeperHost; }

    public String getLocalKafkaBroker() { return localKafkaBroker;    }

    public String getGroupId() {        return groupId;    }

    public String getTopicName() {        return topicName;    }

    public int getFromKafka() {       return fromKafka;    }

    public int getFlink() {        return flink;    }

    public int getBatchsize() {        return batchsize;    }

    public String getMaster() {        return master;    }

    public int getMaxReceiverRate() {        return maxReceiverRate;    }

    public int getKafkaProducer() {        return kafkaProducer;    }

    public int getNewOffset() {        return newOffset;    }

    public int getWorkload() {        return workload;    }

    public String getFilepath() {        return filepath;    }

    public int getNumberRecords() {        return numberRecords;    }

    public int getProduceLoop() {        return produceLoop;    }

    public int getTimeout() {        return timeout;    }

    public int getWriteOutput() {        return writeOutput;    }

    public String getOutputPath() {        return outputPath;    }

    public void setFilepath(String filepath) {        this.filepath = filepath;    }

    public int getClusterMode() {        return clusterMode;    }

    public Conf(){

    try {
        Wini ini = new Wini(new File(confPath));

        windowType = ini.get("window", "window_type", int.class);
        windowSize = ini.get("window", "window_size", int.class);
        windowSlideSize = ini.get("window", "window_slide_size", int.class);
        countSize = ini.get("window", "count_number", int.class);
        servingSpeedFactor = ini.get("kafka", "serving_speed_factor", int.class);
        maxEventDelay = ini.get("kafka", "max_event_delay", int.class);
        localZookeeperHost= ini.get("kafka", "local_zookeeper_host");
        localKafkaBroker= ini.get("kafka", "local_kafka_broker");
        groupId= ini.get("kafka", "group_id");
        topicName= ini.get("kafka", "topic_name");
        master= ini.get("spark", "master");
        filepath= ini.get("kafka", "filepath");
        flink= ini.get("system", "flink", int.class);
        fromKafka= ini.get("system", "from_kafka", int.class);
        batchsize= ini.get("spark", "batchsize", int.class);
        maxReceiverRate=ini.get("spark", "max_receiver_rate", int.class);
        kafkaProducer=ini.get("system", "kafka_producer", int.class);
        newOffset=ini.get("kafka", "new_offset", int.class);
        workload =ini.get("kafka", "workload", int.class);
        numberRecords =ini.get("kafka", "number_records", int.class);
        produceLoop =ini.get("kafka", "produce_loop", int.class);
        timeout =ini.get("system", "timeout", int.class);
        writeOutput =ini.get("system", "write_output", int.class);
        clusterMode =ini.get("system", "cluster_mode", int.class);
        outputPath =ini.get("system", "output_path");
    }catch (IOException e){
        e.printStackTrace();
    }

    }

    public Conf(String newConfPath){

        try {
            Wini ini = new Wini(new File(newConfPath));

            windowType = ini.get("window", "window_type", int.class);
            windowSize = ini.get("window", "window_size", int.class);
            windowSlideSize = ini.get("window", "window_slide_size", int.class);
            countSize = ini.get("window", "count_number", int.class);
            servingSpeedFactor = ini.get("kafka", "serving_speed_factor", int.class);
            maxEventDelay = ini.get("kafka", "max_event_delay", int.class);
            localZookeeperHost= ini.get("kafka", "local_zookeeper_host");
            localKafkaBroker= ini.get("kafka", "local_kafka_broker");
            groupId= ini.get("kafka", "group_id");
            topicName= ini.get("kafka", "topic_name");
            master= ini.get("spark", "master");
            filepath= ini.get("kafka", "filepath");
            flink= ini.get("system", "flink", int.class);
            fromKafka= ini.get("system", "from_kafka", int.class);
            batchsize= ini.get("spark", "batchsize", int.class);
            maxReceiverRate=ini.get("spark", "max_receiver_rate", int.class);
            kafkaProducer=ini.get("system", "kafka_producer", int.class);
            newOffset=ini.get("kafka", "new_offset", int.class);
            workload =ini.get("kafka", "workload", int.class);
            numberRecords =ini.get("kafka", "number_records", int.class);
            produceLoop =ini.get("kafka", "produce_loop", int.class);
            timeout =ini.get("system", "timeout", int.class);
            writeOutput =ini.get("system", "write_output", int.class);
            outputPath =ini.get("system", "output_path");
            clusterMode =ini.get("system", "cluster_mode", int.class);
        }catch (IOException e){
            e.printStackTrace();
        }

    }
}
