package de.tuberlin.main;

import de.tuberlin.io.Conf;
import de.tuberlin.kafka.KafkaProducer;
import de.tuberlin.windows.*;

/**
 * Created by Lehmann on 01.12.2016.
 */
public class Starter {

    public static void main(String[] args) throws Exception {

        Conf conf;
        if (args.length==0){
            conf=  new Conf();
        }else{
            conf = new Conf(args[0]);
        }
        if(args.length==2){
            conf.setFilepath(args[1]);
        }

        //start kafka producer in parallel
        if (conf.getKafkaProducer()==1){
            (new KafkaProducer(conf)).start(); //for writing into kafka
        }


        //start streaming system
        if (conf.getFlink() == 1) {
            if(conf.getClusterMode()==1){
                new FlinkWindowFromKafkaCluster(conf);
            }else{
                new FlinkWindowFromKafka(conf);
            }


        }else if(conf.getFlink() == 0){
            if(conf.getClusterMode()==1){
                new SparkWindowFromKafkaCluster(conf);
            }else{
                new SparkWindowFromKafka(conf);
            }

        }
    }
}


