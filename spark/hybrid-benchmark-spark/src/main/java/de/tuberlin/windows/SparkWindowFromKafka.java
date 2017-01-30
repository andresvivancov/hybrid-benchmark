package de.tuberlin.windows;

import de.tuberlin.io.Conf;
import de.tuberlin.serialization.SparkStringTsDeserializer;
import de.tuberlin.io.TaxiRideClass;
import org.apache.kafka.common.serialization.StringDeserializer;

//import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.kafka010.*;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple6;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.*;

/**
 * Created by patrick on 15.12.16.
 */
public class SparkWindowFromKafka implements Serializable{

    public SparkWindowFromKafka(Conf conf) throws Exception{

        //spark.streaming.kafka.maxRatePerPartition : Define messages per second to retrive from kafka
        final String LOCAL_ZOOKEEPER_HOST = conf.getLocalZookeeperHost();
        final String APPLICATION_NAME="Spark Window";
        final String LOCAL_KAFKA_BROKER = conf.getLocalKafkaBroker();
        final String GROUP_ID = conf.getGroupId();
        final String TOPIC_NAME="spark-"+conf.getTopicName();
        final String MASTER=conf.getMaster();


        final int batchsize = conf.getBatchsize();         //size of elements in each window
        final int windowTime = conf.getWindowSize();          //measured in seconds
        final int slidingTime = conf.getWindowSlideSize();          //measured in seconds
        final int partitions = 1;
        final int multiplication_factor=1;
        final String id= new BigInteger(130,new SecureRandom()).toString(32);

        Map<String,Integer> topicMap = new HashMap<>();
        topicMap.put("winagg",partitions);

        SparkConf sparkConf = new SparkConf()
                .setAppName(APPLICATION_NAME)
              // .set("spark.streaming.kafka.maxRatePerPartition",String.valueOf(conf.getWorkload()))
              //  .set("spark.streaming.backpressure.enabled","true")
                .set("spark.streaming.backpressure.initialRate","1000")
                .setMaster(MASTER);
        System.out.println("Starting reading from "+TOPIC_NAME);
       JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(batchsize*multiplication_factor));

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




        final JavaInputDStream<ConsumerRecord<String,String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferBrokers(),
               // ConsumerStrategies.Assign(topics,kafkaParams)
                ConsumerStrategies.<String,String>Subscribe(topics,kafkaParams)
        );






        JavaDStream<Tuple4<Double, Long, Long,Long>> averagePassengers=messages
        //message

                .map(x->new Tuple3<Long,Long,Long>(
                       1L,Long.valueOf(TaxiRideClass.fromString(x.value()).passengerCnt)
                            ,TaxiRideClass.fromString(x.value()).timestamp))

                .window(new Duration(windowTime*multiplication_factor),new Duration(slidingTime*multiplication_factor))
               .reduce( (x,y)-> new Tuple3<Long, Long, Long>(x._1()+y._1(),x._2()+y._2(),x._3()<y._3()?y._3():x._3() ) )

               .map(x->new Tuple4<Double, Long, Long,Long>(new Double(x._2()*1000/x._1())/1000.0,x._1(),System.currentTimeMillis()-x._3(),System.currentTimeMillis()));




        String path=conf.getOutputPath()+"spark/";
        String fileName=windowTime+"/"+slidingTime+"/"+conf.getWorkload()+"/"+"file_"+batchsize;
        String suffix="";

        if(conf.getWriteOutput()==0){
            averagePassengers.print();
        }else if(conf.getWriteOutput()==1){
            averagePassengers.map(x->new Tuple6<>(",",x._1(),x._2(),x._3(),x._4(),","))
                    .dstream().saveAsTextFiles(path+fileName,suffix);
        }else if(conf.getWriteOutput()==2){
            averagePassengers.print();
            averagePassengers.map(x->new Tuple6<>(",",x._1(),x._2(),x._3(),x._4(),","))
                    .dstream().saveAsTextFiles(path+fileName,suffix);
        }

        jssc.start();

       // jssc.awaitTermination();
        jssc.awaitTerminationOrTimeout(conf.getTimeout());
        jssc.stop();
    }



}
