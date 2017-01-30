package de.tuberlin.kafka;

import de.tuberlin.io.Conf;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Created by patrick on 21.12.16.
 */
public class KafkaProducer extends Thread{
Conf conf;
    public KafkaProducer(Conf conf){
        this.conf=conf;
    }
public void run(){

        try {
            if (conf.getProduceLoop()==1){
               writeToKafkaLoop(conf);
            }else {
                writeToKafkaDelayed(conf);
            }
        }catch(Exception e) {
            e.printStackTrace();
        }



    }

    public void writeToKafkaDelayed(Conf conf) throws IOException,InterruptedException {


        final String FILEPATH = conf.getFilepath();
        String systemname="spark";
        if (conf.getFlink() == 1) {
            systemname="flink";
        }else if(conf.getFlink() == 4){
            systemname="flink";
        }
        final String TOPIC_NAME = systemname+"-"+conf.getTopicName();
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("acks", "all");
        kafkaProps.put("retries", "0");
        kafkaProps.put("batch.size", "16384");
        kafkaProps.put("linger.ms", "1");
        kafkaProps.put("buffer.memory", "3354432");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Long fileSize=Files.lines(Paths.get(FILEPATH)).count();
        int maxRate=conf.getWorkload();
        final int numberLoops=conf.getNumberRecords()/maxRate;
        final Long numberFiles=maxRate/fileSize;
        final Long rest=maxRate%fileSize;
        //System.out.println("jjjj  "+SLEEPMILLIS+" : "+SLEEPNANOS);
        //System.out.println("iiii "+numberLoops+" "+numberFiles+" rest "+rest);
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(kafkaProps);

            this.sleep(4000);

            for (int i = 0; i < numberLoops; i++) {
                Long start = System.currentTimeMillis();
                for (int j = 0; j < numberFiles; j++) {
                          Files.lines(Paths.get(FILEPATH)).forEach(x->{
                                producer.send(new ProducerRecord<String, String>(TOPIC_NAME,"1",x));
                          });
                }
                Files.lines(Paths.get(FILEPATH)).limit(rest).forEach(x-> {
                    producer.send(new ProducerRecord<String, String>(TOPIC_NAME, "1", x));
                });
                Long end=System.currentTimeMillis();
                Long sleepTime=999-(end-start);
                //System.out.println(end+" "+start+" "+(end-start)+" "+sleepTime);
                if (sleepTime<0){sleepTime=0L;}

                this.sleep(sleepTime);

                }

        }

    public void writeToKafkaLoop(Conf conf) throws IOException,InterruptedException {

        String systemname="spark";
        if (conf.getFlink() == 1) {
            systemname="flink";
        }else if(conf.getFlink() == 4){
        systemname="flink";
        }
        final String TOPIC_NAME = systemname+"-"+conf.getTopicName();
        final String FILEPATH = conf.getFilepath();

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", conf.getLocalKafkaBroker());
        kafkaProps.put("acks", "all");
        kafkaProps.put("retries", "0");
        kafkaProps.put("batch.size", "16384");
        kafkaProps.put("linger.ms", "1");
        kafkaProps.put("buffer.memory", "3354432");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        System.out.println("Starting writing to "+TOPIC_NAME);
        Long fileSize=Files.lines(Paths.get(FILEPATH)).count();
        int maxRate=conf.getWorkload();
        final int numberLoops=conf.getNumberRecords()/maxRate;
        final Long numberFiles=maxRate/fileSize;
        final Long rest=maxRate%fileSize;
        //System.out.println("jjjj  "+SLEEPMILLIS+" : "+SLEEPNANOS);
        //System.out.println("iiii "+numberLoops+" "+numberFiles+" rest "+rest);
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(kafkaProps);

        this.sleep(4000);
        Long startWriting=System.currentTimeMillis();
        Long difference=Long.valueOf(conf.getTimeout());
        while(System.currentTimeMillis()-startWriting<difference){

            Long start = System.currentTimeMillis();
            for (int j = 0; j < numberFiles; j++) {
                Files.lines(Paths.get(FILEPATH)).forEach(x->{
                    producer.send(new ProducerRecord<String, String>(TOPIC_NAME,"1",x));
                });
            }
            Files.lines(Paths.get(FILEPATH)).limit(rest).forEach(x-> {
                producer.send(new ProducerRecord<String, String>(TOPIC_NAME, "1", x));
            });
            Long end=System.currentTimeMillis();
            Long sleepTime=999-(end-start);
            //System.out.println(end+" "+start+" "+(end-start)+" "+sleepTime);
            if (sleepTime<0){sleepTime=0L;}

            this.sleep(sleepTime);

        }

    this.interrupt();

    }


}
