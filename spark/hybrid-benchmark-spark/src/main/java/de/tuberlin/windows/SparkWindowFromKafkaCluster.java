package de.tuberlin.windows;

import de.tuberlin.io.Conf;
import de.tuberlin.io.TaxiRideClass;
import de.tuberlin.serialization.SparkStringTsDecoder;
import de.tuberlin.serialization.SparkStringTsDeserializer;
import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple6;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.*;

//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Created by patrick on 15.12.16.
 */
public class SparkWindowFromKafkaCluster implements Serializable{

    public SparkWindowFromKafkaCluster(Conf conf) throws Exception{

        //spark.streaming.kafka.maxRatePerPartition : Define messages per second to retrive from kafka
        final String LOCAL_ZOOKEEPER_HOST = conf.getLocalZookeeperHost();
        final String APPLICATION_NAME="Spark Window";
        final String LOCAL_KAFKA_BROKER = conf.getLocalKafkaBroker();
        final String GROUP_ID = conf.getGroupId();
        final String TOPIC_NAME="spark-"+conf.getTopicName();
        final String MASTER=conf.getMaster();
        System.out.println("Starting reading from "+TOPIC_NAME);

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
                .set("spark.streaming.backpressure.enabled","true")
                //.set("spark.streaming.backpressure.initialRate","1000")
                .setMaster(MASTER);


        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(batchsize*multiplication_factor));
/*
        Set<String> topics = Collections.singleton(TOPIC_NAME);
        // Collection<String> topics=Arrays.asList(TOPIC_NAME,"win","winagg");
        //Collection<TopicPartition> topics=Arrays.asList(new TopicPartition[]{new TopicPartition(TOPIC_NAME,1)});

        // Map<String, String>kafkaParams=new HashMap<>();
        Map<String,String>kafkaParams=new HashMap<>();
        // kafkaParams.put("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
        //kafkaParams.put("metadata.broker.list",LOCAL_KAFKA_BROKER);
        kafkaParams.put("bootstrap.servers",LOCAL_KAFKA_BROKER);
        kafkaParams.put("auto.offset.reset","largest");
        kafkaParams.put("enable.auto.commit","true");
        //  kafkaParams.put("enable.auto.commit","true");
        if(conf.getNewOffset()==1){ kafkaParams.put("group.id", id);}else{
            kafkaParams.put("group.id", conf.getGroupId());
        }
        //   kafkaParams.put("key.deserializer", StringDeserializer.class);
        //   kafkaParams.put("value.deserializer", SparkStringTsDeserializer.class);



        final JavaPairInputDStream<String,String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                SparkStringTsDecoder.class,
                kafkaParams,
                topics
        );
*/

        Collection<String> topics=Arrays.asList(TOPIC_NAME,"win","winagg");
        //Collection<TopicPartition> topics=Arrays.asList(new TopicPartition[]{new TopicPartition(TOPIC_NAME,1)});

        // Map<String, String>kafkaParams=new HashMap<>();
        Map<String,Object>kafkaParams=new HashMap<>();
        // kafkaParams.put("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
        //kafkaParams.put("metadata.broker.list",LOCAL_KAFKA_BROKER);
        kafkaParams.put("bootstrap.servers",LOCAL_KAFKA_BROKER);
        kafkaParams.put("auto.offset.reset","latest");
        kafkaParams.put("enable.auto.commit","true");
        if(conf.getNewOffset()==1){ kafkaParams.put("group.id", id);}else{
            kafkaParams.put("group.id", conf.getGroupId());
        }
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", SparkStringTsDeserializer.class);

        final JavaInputDStream<ConsumerRecord<String,String>> messages = org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferBrokers(),
                // ConsumerStrategies.Assign(topics,kafkaParams)
                ConsumerStrategies.<String,String>Subscribe(topics,kafkaParams)
        );


        //Functions
        Function2<Tuple3<Long,Long,Long>,Tuple3<Long,Long,Long>,Tuple3<Long,Long,Long>> reduceF=
                new Function2<Tuple3<Long,Long,Long>, Tuple3<Long,Long,Long>, Tuple3<Long,Long,Long>>(){
                    @Override
                    public Tuple3<Long, Long, Long> call(Tuple3<Long, Long, Long> x, Tuple3<Long, Long, Long> y) throws Exception {
                        return new Tuple3<>(x._1()+y._1(),x._2()+y._2(),x._3()<y._3()?y._3():x._3() ) ;
                    }
                };

        Function<Tuple2<Integer,Tuple3<Long,Long,Long>>,Tuple4<Double,Long,Long,Long>> mapF=new Function<Tuple2<Integer,Tuple3<Long, Long, Long>>, Tuple4<Double, Long, Long, Long>>() {
            @Override
            public Tuple4<Double, Long, Long, Long> call(Tuple2<Integer,Tuple3<Long, Long, Long>> x) throws Exception {
                return new Tuple4<>(new Double(x._2()._2()*1000/x._2()._1())/1000.0,x._2()._1(),System.currentTimeMillis()-x._2()._3(),System.currentTimeMillis());
            }
        };

        PairFunction<Tuple3<Long,Long,Long>, Integer, Tuple3<Long, Long, Long>> pairF=
                new PairFunction<Tuple3<Long,Long,Long>, Integer, Tuple3<Long, Long, Long>>() {
                    @Override
                    public Tuple2<Integer, Tuple3<Long, Long, Long>> call(Tuple3<Long, Long, Long> t) throws Exception {
                        int ran=new Random().nextInt(9)+1;
                        return new Tuple2<Integer, Tuple3<Long, Long, Long>>(ran,t);
                    }
                };


        //Aggregations

        JavaDStream<Tuple4<Double, Long, Long,Long>> averagePassengers=messages
                .map(x->new Tuple3<Long,Long,Long>(1L,Long.valueOf(TaxiRideClass.fromString(x.value()).passengerCnt)
                        ,TaxiRideClass.fromString(x.value()).timestamp))

                .mapToPair(pairF)

                .reduceByKeyAndWindow(reduceF,Durations.milliseconds(windowTime),Durations.milliseconds(slidingTime))

                .map(mapF);


        //printing output
        String path=conf.getOutputPath()+"spark/";
        String fileName=windowTime+"/"+slidingTime+"/"+conf.getWorkload()+"/"+"file_"+batchsize;
        String suffix="";
        if(conf.getWriteOutput()==0){
            // print result on stdout
            averagePassengers.print();
        }else if(conf.getWriteOutput()==1){
            averagePassengers.map(x->new Tuple6<>(",",x._1(),x._2(),x._3(),x._4(),","))
                    .dstream().saveAsTextFiles(path+fileName,suffix);
        }else if(conf.getWriteOutput()==2){
            averagePassengers.print();
            averagePassengers.map(x->new Tuple6<>(",",x._1(),x._2(),x._3(),x._4(),","))
                    .dstream().saveAsTextFiles(path+fileName,suffix);
        }


        //starting environment
        jssc.start();

        // jssc.awaitTermination();
        jssc.awaitTerminationOrTimeout(conf.getTimeout());
        jssc.stop();
    }



}
