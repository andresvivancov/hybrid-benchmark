package de.tuberlin.windows;

import de.tuberlin.io.Conf;
import de.tuberlin.serialization.SparkStringTsDeserializer;
import de.tuberlin.io.TaxiRideClass;
import org.apache.kafka.common.serialization.StringDeserializer;

//import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import org.apache.spark.streaming.kafka010.*;
import org.json4s.DefaultWriters;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple6;

import javax.sound.midi.SysexMessage;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by patrick on 15.12.16.
 */
public class SparkWindowFromKafkaCluster implements Serializable{

    public SparkWindowFromKafkaCluster(Conf conf) throws Exception{

        //setup parameters
        final String LOCAL_ZOOKEEPER_HOST = conf.getLocalZookeeperHost();
        final String APPLICATION_NAME="Spark Window";
        final String LOCAL_KAFKA_BROKER = conf.getLocalKafkaBroker();
        final String GROUP_ID = conf.getGroupId();
        final String TOPIC_NAME="spark-"+conf.getTopicName();
        final String MASTER=conf.getMaster();


        final int batchsize = conf.getBatchsize();         //size of elements in each window
        final int slidingTime = conf.getWindowSlideSize();          //measured in seconds
        final int windowTime = conf.getWindowSize();          //measured in seconds
        final int partitions = 1;
        final int multiplication_factor=1;
        final String id= new BigInteger(130,new SecureRandom()).toString(32);
        final String BATCH_PATH=conf.getBatchpath();

        //setup spark
        Map<String,Integer> topicMap = new HashMap<>();
        topicMap.put("winagg",partitions);


        SparkConf sparkConf = new SparkConf()
                .setAppName(APPLICATION_NAME)
                // .set("spark.streaming.kafka.maxRatePerPartition",String.valueOf(conf.getWorkload()))
                .set("spark.streaming.backpressure.enabled","true")
                .set("spark.streaming.backpressure.initialRate","1000")
                .setMaster(MASTER);
        System.out.println("Starting reading from "+TOPIC_NAME);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(windowTime)); //batchsize = max = windowtime


        //set up kafka connection
        Collection<String> topics=Arrays.asList(TOPIC_NAME,"win","winagg");
        Map<String,Object>kafkaParams=new HashMap<>();
        kafkaParams.put("bootstrap.servers",LOCAL_KAFKA_BROKER);
        kafkaParams.put("auto.offset.reset","latest");
        kafkaParams.put("enable.auto.commit","true");
        if(conf.getNewOffset()==1){ kafkaParams.put("group.id", id);}else{
            kafkaParams.put("group.id", conf.getGroupId());
        }
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", SparkStringTsDeserializer.class);




        //make list with elements from textfile with size specified on config file
        //Stream<String> fileinputstream=Files.lines(Paths.get(BATCH_PATH));

        /*Long filesize=fileinputstream.count();
        Long numberLoops = conf.getBatchfilesize()/filesize;
        Long offset=conf.getBatchfilesize()%filesize;
        List list= Files.lines(Paths.get(BATCH_PATH)).limit(offset).collect(Collectors.toList());
        for (int i = 0; i < numberLoops; i++) {
            list.addAll( Files.lines(Paths.get(BATCH_PATH)).collect(Collectors.toList()));
        }
        fileinputstream.close();

        //create batchfile with the created list
        JavaRDD<String> lines=sc.parallelize(list);
        lines.cache();
        // JavaRDD<String> lines = sc.textFile(BATCH_PATH);


        //create PairRdd out of the input Rdd for using keyed aggregation(join)
        JavaPairRDD<String, String> batchFile = lines.keyBy(new Function<String,String>(){
            @Override
            public String call(String arg0) throws Exception {
                return arg0.split(",")[0];
            }
        });

        //cache the batchfile so it does not read every time
        batchFile.cache();
        // batchFile.persist(StorageLevel.MEMORY_AND_DISK());
        //  Broadcast<JavaPairRDD<String,String>> broadBatch=sc.broadcast(batchFile);

    */


       // JavaPairRDD<String, String> batchFile = sc.textFile(BATCH_PATH);
        JavaPairRDD<String, String> batchFile = sc.textFile(BATCH_PATH).keyBy(new Function<String,String>(){
            @Override
            public String call(String arg0) throws Exception {
                return arg0.split(",")[0];
            }
        });
       // System.out.println("dddddd  "+batchFile.count());
        //batchFile.cache();
        JavaPairRDD<String, String> cachedBatch= batchFile.cache();
        //batchFile.repartition(8);
      //  System.out.println("ddd :"+batchFile.getNumPartitions());
        //Broadcast<JavaPairRDD<String,String>> broadBatch=sc.broadcast(batchFile);
      //  Broadcast<JavaPairRDD<String,String>> broadBatch=sc.broadcast(cachedBatch);



        //create kafka source
        final JavaInputDStream<ConsumerRecord<String,String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferBrokers(),
                // ConsumerStrategies.Assign(topics,kafkaParams)
                ConsumerStrategies.<String,String>Subscribe(topics,kafkaParams)
        );


        //receive data stream from kafka
        JavaPairDStream<String, String> stream = messages
                .mapToPair(x->new Tuple2<String, String>(x.value().split(",")[0],x.value()))
                ;

        //create window in datastream
        JavaPairDStream<String, String> windowedStream = stream.window(Durations.milliseconds(windowTime));


        //join the datastream with batchfile

        JavaPairDStream<String, String> joinedStream = windowedStream.transformToPair(
                new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
                    @Override
                    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) {
                       // JavaPairRDD<String,String> joined=batchFile.join(rdd)
                       // JavaPairRDD<String,String> joined=broadBatch.value().join(rdd,30)
                        JavaPairRDD<String,String> joined=rdd.join(cachedBatch,conf.getCountSize())
                        //JavaPairRDD<String,String> joined=rdd.join(batchFile)
                       // JavaPairRDD<String,String> joined=rdd.join(broadBatch.value(),conf.getCountSize())

                                .mapValues(x->x._1.concat(","+String.valueOf(System.currentTimeMillis()-Long.valueOf(x._1.split(",")[9]))));
                        return joined;
                        //return rdd;
                    }
                }
        );


        //print results
        //joinedStream.print();


        //printing output
        String path=conf.getOutputPath()+"spark/";
        String fileName=windowTime+"/"+conf.getBatchfilesize()+"/"+conf.getWorkload()+"/"+"file_"+batchsize;
        String suffix="";
        if(conf.getWriteOutput()==0){
            // print result on stdout
            joinedStream.print();
        }else if(conf.getWriteOutput()==1){
            joinedStream.map(x->new Tuple4<>(",",x._1(),x._2(),","))
                    .dstream().saveAsTextFiles(path+fileName,suffix);
        }else if(conf.getWriteOutput()==2){
            joinedStream.print();
            joinedStream.map(x->new Tuple4<>(",",x._1(),x._2(),","))
                    .dstream().saveAsTextFiles(path+fileName,suffix);
        }



        //start spark
        jssc.start();

        // jssc.awaitTermination();
        jssc.awaitTerminationOrTimeout(conf.getTimeout());
        jssc.stop();
    }



}
