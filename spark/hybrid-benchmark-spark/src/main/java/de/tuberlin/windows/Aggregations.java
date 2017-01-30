package de.tuberlin.windows;


import de.tuberlin.io.TaxiRideClass;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Random;

/**
 * Created by Lehmann on 01.12.2016.
 */
public class Aggregations {



    /**
     Maps Taxiride so just id of ride and passengercount stays
     */
    public static class MapPassenger implements MapFunction<String, Tuple4<Integer,Long, Long,Long>> {


        @Override
        public Tuple4<Integer,Long, Long,Long> map(String line) throws Exception {
            TaxiRideClass taxi=TaxiRideClass.fromString(line);
            int ran=new Random().nextInt(9)+1;
            return new Tuple4<Integer,Long,Long,Long>(1,1L, Long.valueOf(taxi.passengerCnt),taxi.timestamp );

        }
    }

    /**
     Maps Taxiride so just id of ride and passengercount stays
     */
    public static class MapPassengerClust implements MapFunction<String, Tuple4<Integer,Long, Long,Long>> {


        @Override
        public Tuple4<Integer,Long, Long,Long> map(String line) throws Exception {
            TaxiRideClass taxi=TaxiRideClass.fromString(line);
            int ran=new Random().nextInt(9)+1;
            return new Tuple4<Integer,Long,Long,Long>(ran,1L, Long.valueOf(taxi.passengerCnt),taxi.timestamp );

        }
    }
    /**
     Maps Taxiride so just id of ride and passengercount stays
     */
    public static class MapToMean implements MapFunction< Tuple4<Integer,Long, Long,Long> , Tuple4<Double, Long,Long,Long>> {


        public Tuple4<Double,Long,Long,Long> map( Tuple4<Integer,Long, Long,Long> t) throws Exception {
            Long millis=System.currentTimeMillis();

            Long duration=millis-t.f3;
            return new Tuple4<Double,Long,Long,Long>(Math.round(t.f2/new Double(t.f1)*1000)/1000.0,t.f1, duration,millis);

        }
    }

    /**
     Maps Taxiride so just id of ride and passengercount stays
     */
    public static class MapToMean2 implements MapFunction< Tuple3<Integer, Integer,Long> , Tuple3<Double, Double,Long>> {


        public Tuple3<Double, Double,Long> map( Tuple3<Integer, Integer,Long> t) throws Exception {
            Long millis=System.currentTimeMillis();
            String timeStamp = new Date(millis).toString();
            Long duration=millis-t.f2;

            return new Tuple3<Double,Double,Long>(Math.round(t.f1/new Double(t.f0)*1000)/1000.0,Double.valueOf(t.f0), millis);

        }
    }

    /**
     Maps Taxiride so just id of ride and passengercount stays
     */
    public static class MapOutput implements MapFunction< Tuple4<Double, Long,Long,Long> , Tuple6<String, Double, Long,Long,Long,String>> {


        public Tuple6<String,Double,Long,Long,Long,String> map( Tuple4<Double, Long,Long,Long>  t) throws Exception {


            return new Tuple6<String,Double,Long,Long,Long,String>(",",t.f0,t.f1,t.f2,t.f3,",");

        }
    }


    public static class SumAllValues implements ReduceFunction<Tuple4<Integer,Long, Long,Long>> {
        @Override
        public Tuple4<Integer,Long, Long,Long> reduce(Tuple4<Integer,Long, Long,Long> value1, Tuple4<Integer,Long, Long,Long> value2) throws Exception {
           Long time=value1.f3;
            if(value1.f3<value2.f3){
                time=value2.f3;
            }
            return new Tuple4<Integer,Long, Long,Long>(value1.f0,value1.f1+value2.f1, value1.f2+value2.f2,time);
        }
    }







    /**
     * Returns the average number of passengers in a specific time window
     */
    public static class PassengerCounter implements WindowFunction<
            Tuple3<Integer, Integer,Long>, // input type
            Tuple3<Double,String,Long>, // output type
            Tuple, // key type
            TimeWindow> // window type
    {

        @SuppressWarnings("unchecked")
        @Override
        public void apply(
                Tuple key,
                TimeWindow window,
                Iterable<Tuple3<Integer, Integer,Long>> values,
                Collector<Tuple3<Double,String,Long>> out) throws Exception {

//            Long cellId = ((Tuple2<Long, Integer>)key).f0;
            //           Integer passenger = ((Tuple2<Long, Integer>)key).f1;
            long windowTime = window.getStart();
            String time=new Date(window.getStart()).toString()+" "+new Date(window.getEnd()).toString()+" "+new Date(window.maxTimestamp()).toString();
            String dateString=new Date(windowTime).toString();
            Double cnt = 0.0;
            Double sum = 0.0;

            for(Tuple3<Integer, Integer,Long> v : values) {
                cnt += 1;
                sum += v.f1;
            }

            Date timeStamp= new Date(System.currentTimeMillis());
            Long duration= Long.valueOf(System.currentTimeMillis()-window.maxTimestamp());
            out.collect(new Tuple3<>(Double.valueOf( Math.round(sum/cnt*1000.0)/1000.0),String.valueOf(cnt),duration ));
            //out.collect(new Tuple1<>( Double.valueOf( Math.round(cnt*100.0)/100.0)));
        }
    }

}

