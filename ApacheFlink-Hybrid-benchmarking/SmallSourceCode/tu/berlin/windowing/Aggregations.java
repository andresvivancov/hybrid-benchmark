package org.apache.flink.streaming.examples.kafka.tu.berlin.windowing;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.examples.kafka.tu.berlin.io.TaxiRideClass;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Random;

/**
 * Created by Lehmann on 01.12.2016.
 */
public class Aggregations {

	/**
	 * Andres Methods
	 */


	public static class MapHistoricalDatatoID1 extends RichMapFunction<String, Tuple2<String, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Long> map(String record) {
			String[] data = record.split(",");
			return new Tuple2<>(String.valueOf(data[0]), Long.valueOf(data[0]));
		}

	}
		public static class MapHistoricalDatatoID2 extends RichMapFunction<String, Tuple3<Long, String, Long>> {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple3<Long, String, Long> map(String record) {
				String[] data = record.split(",");
				return new Tuple3<>(Long.valueOf(data[0]), String.valueOf(data[0]), System.currentTimeMillis());
			}


		}


	public static class MapHistoricalDatatoID3 extends RichMapFunction<String, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long map(String record) {
			String[] data = record.split(",");
			return new Long(Long.valueOf(data[0]));
		}

	}


	public static class MapTupletoString extends RichMapFunction<String, String> {

		@Override
		public String map(String line) throws Exception {
			TaxiRideClass taxi = TaxiRideClass.fromString(line);
			return new String(taxi.toString());

		}

	}

//Function for tuple 10  Taxi after join streaming
public static class MapTupleToTaxi extends RichMapFunction<String, TaxiRideClass> {

	@Override
	public TaxiRideClass map(String line) throws Exception {
		TaxiRideClass taxi = TaxiRideClass.fromString(line);
		return taxi;

	}

}

	//Function for tuple 10  Taxi after join streaming
	public static class MapTaxitoTuple10 extends RichMapFunction<String, Tuple10<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long>> {

		@Override
		public Tuple10<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long> map(String line) throws Exception {
			TaxiRideClass taxi = TaxiRideClass.fromString(line);
			return new Tuple10<>(taxi.rideId,taxi.isStart,taxi.startTime,taxi.endTime,taxi.startLon,taxi.startLat,taxi.endLon,taxi.endLat,
				taxi.passengerCnt,taxi.timestamp);

		}

	}


	public static class MapTimeStampStreaming implements MapFunction<String, Tuple2<String,Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Long> map(String line) throws Exception {
			TaxiRideClass taxi = TaxiRideClass.fromString(line);
			return new Tuple2<>(taxi.toString(), System.currentTimeMillis());

		}
	}


	public static class MapTimeStampStreaming2 implements MapFunction<String, Tuple3<Long,String,Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<Long,String, Long> map(String line) throws Exception {
			TaxiRideClass taxi = TaxiRideClass.fromString(line);
			return new Tuple3<>(taxi.rideId,taxi.toString(), System.currentTimeMillis());

		}
	}

	/**
	 * ********************************  Windows Calculation Andres   *******************************************888
	 */

	public static class MapPassengerH implements MapFunction<String,  Tuple15<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>> {


		@Override
		public Tuple15<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long> map(String line) throws Exception {
			TaxiRideClass taxi = TaxiRideClass.fromString(line);
			int ran = new Random().nextInt(9) + 1;
			return new Tuple15<>(taxi.rideId,taxi.isStart,taxi.startTime,taxi.endTime,taxi.startLon,taxi.startLat,taxi.endLon,taxi.endLat,
				taxi.passengerCnt,taxi.timestamp,ran,1L,Long.valueOf(taxi.passengerCnt), taxi.timestamp,1L);

		}
	}

	public static class MapPassengerR implements MapFunction<String,  Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>> {


		@Override
		public Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long> map(String line) throws Exception {
			TaxiRideClass taxi = TaxiRideClass.fromString(line);
			int ran = new Random().nextInt(9) + 1;
			int rideint = (int) taxi.rideId;
			return new Tuple15<>(rideint,taxi.isStart,
								taxi.startTime,taxi.endTime,
								taxi.startLon,taxi.startLat,
								taxi.endLon,taxi.endLat,
							taxi.passengerCnt,taxi.timestamp,
				1,1L,Long.valueOf(taxi.passengerCnt), taxi.timestamp,1L);

		}
	}

	public static class MapPassengerR2 implements MapFunction<String,  Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>> {


		@Override
		public Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long> map(String line) throws Exception {
			TaxiRideClass taxi = TaxiRideClass.fromString(line);
			int ran = new Random().nextInt(9) + 1;
			int rideint = (int) taxi.rideId;
			return new Tuple15<>(rideint,taxi.isStart,
				taxi.startTime,taxi.endTime,
				taxi.startLon,taxi.startLat,
				taxi.endLon,taxi.endLat,
				taxi.passengerCnt,taxi.timestamp,
				1,1L,Long.valueOf(taxi.passengerCnt), taxi.timestamp,1L);

		}
	}

	public static class SumAllValuesH implements ReduceFunction<Tuple15<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>> {
		@Override
		public Tuple15<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>
		reduce(Tuple15<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long> value1,
				Tuple15<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long> value2) throws Exception {
			Long time = value1.f13;
			if (value1.f13 < value2.f13) {
				time = value2.f13;
			}
			return new Tuple15<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>
				(value1.f0,value1.f1,value1.f2,value1.f3,value1.f4,value1.f5,
				 value1.f6,value1.f7,value1.f8,value1.f9, value1.f10,value1.f11 + value2.f11, value1.f12 + value2.f12, time,value1.f14);
		}
	}

	public static class SumAllValuesR implements ReduceFunction<Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>> {
		@Override
		public Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>
		reduce(Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long> value1,
			   Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long> value2) throws Exception {
			Long time = value1.f9;
			Long timemayor = value1.f9;
			if (value1.f9 < value2.f9) {
				time = value2.f9;
				timemayor = value1.f9;
			}
			return new Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>
				(value1.f0,value1.f1,value1.f2,value1.f3,value1.f4,value1.f5,
					value1.f6,value1.f7,value1.f8,value2.f9, value1.f10+value2.f10
							, time, timemayor,(System.currentTimeMillis()-timemayor)/1000
								,(System.currentTimeMillis()-timemayor));
		}
	}

	public static class SumAllValuesR2 implements ReduceFunction<Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>> {
		@Override
		public Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>
		reduce(Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long> value1,
			   Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long> value2) throws Exception {
			Long time = value1.f9;
			Long timemayor = value1.f9;
			if (value1.f9 < value2.f9) {
				time = value2.f9;
				timemayor = value1.f9;
			}
			return new Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>
				(value1.f0,value1.f1,value1.f2,value1.f3,value1.f4,value1.f5,
					value1.f6,value1.f7,value1.f8,value2.f9,
					value1.f10+value2.f10
					, time, timemayor,(System.currentTimeMillis()-timemayor)/1000
					,(System.currentTimeMillis()-timemayor));
		}
	}

	public static class SumAllValuesborrar implements ReduceFunction<Tuple4<Integer, Long, Long, Long>> {
		@Override
		public Tuple4<Integer, Long, Long, Long> reduce(Tuple4<Integer, Long, Long, Long> value1, Tuple4<Integer, Long, Long, Long> value2) throws Exception {
			Long time = value1.f3;
			if (value1.f3 < value2.f3) {
				time = value2.f3;
			}
			return new Tuple4<Integer, Long, Long, Long>(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2, time);
		}
	}



	public static class MapToMeanH implements MapFunction<Tuple15<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>,
		Tuple15<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long,Double,Long, Long, Long, Long>> {

		public Tuple15<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long,Double,Long, Long, Long, Long>
		map(Tuple15<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long> t) throws Exception {
			Long millis = System.currentTimeMillis();
			Long duration = millis - t.f13;
			Long seconds = duration/1000;
			return new Tuple15<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long,Double,Long, Long, Long, Long>
				(t.f0,t.f1,t.f2,t.f3,t.f4,t.f5,t.f6,t.f7,t.f8,t.f9,(
					Math.round(t.f12 / new Double(t.f11) * 1000) / 1000.0), t.f11, duration, millis,seconds);

		}
	}




	public static class MapToMeanR implements MapFunction<Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>,
		Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Double,Long, Long, Long, Long>> {

		public Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Double,Long, Long, Long, Long>
		map(Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long> t) throws Exception {
			Long millis = System.currentTimeMillis();
			Long duration = millis - t.f9;
			Long seconds = duration/1000;
			return new Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Double,Long, Long, Long, Long>
				(t.f0,t.f1,t.f2,t.f3,t.f4,t.f5,t.f6,t.f7,t.f8,t.f9,(
					Math.round(t.f12 / new Double(t.f11) * 1000) / 1000.0), t.f11, millis,seconds,duration);

		}
	}




	/**
	 * ******************************** **************************8   *******************************************888
	 */


	/**
		 * Maps Taxiride so just id of ride and passengercount stays
		 */
		public static class MapPassenger implements MapFunction<String, Tuple4<Integer, Long, Long, Long>> {


			@Override
			public Tuple4<Integer, Long, Long, Long> map(String line) throws Exception {
				TaxiRideClass taxi = TaxiRideClass.fromString(line);
				int ran = new Random().nextInt(9) + 1;
				return new Tuple4<Integer, Long, Long, Long>(ran, 1L, Long.valueOf(taxi.passengerCnt), taxi.timestamp);

			}
		}

		/**
		 * Maps Taxiride so just id of ride and passengercount stays
		 */
		public static class MapPassengerClust implements MapFunction<String, Tuple4<Integer, Long, Long, Long>> {


			@Override
			public Tuple4<Integer, Long, Long, Long> map(String line) throws Exception {
				TaxiRideClass taxi = TaxiRideClass.fromString(line);
				int ran = new Random().nextInt(9) + 1;
				return new Tuple4<Integer, Long, Long, Long>(ran, 1L, Long.valueOf(taxi.passengerCnt), taxi.timestamp);

			}
		}

		/**
		 * Maps Taxiride so just id of ride and passengercount stays
		 */
		public static class MapToMean implements MapFunction<Tuple4<Integer, Long, Long, Long>, Tuple4<Double, Long, Long, Long>> {


			public Tuple4<Double, Long, Long, Long> map(Tuple4<Integer, Long, Long, Long> t) throws Exception {
				Long millis = System.currentTimeMillis();

				Long duration = millis - t.f3;
				return new Tuple4<Double, Long, Long, Long>(Math.round(t.f2 / new Double(t.f1) * 1000) / 1000.0, t.f1, duration, millis);

			}
		}

		/**
		 * Maps Taxiride so just id of ride and passengercount stays
		 */
		public static class MapToMean2 implements MapFunction<Tuple3<Integer, Integer, Long>, Tuple3<Double, Double, Long>> {


			public Tuple3<Double, Double, Long> map(Tuple3<Integer, Integer, Long> t) throws Exception {
				Long millis = System.currentTimeMillis();
				String timeStamp = new Date(millis).toString();
				Long duration = millis - t.f2;

				return new Tuple3<Double, Double, Long>(Math.round(t.f1 / new Double(t.f0) * 1000) / 1000.0, Double.valueOf(t.f0), millis);

			}
		}

		/**
		 * Maps Taxiride so just id of ride and passengercount stays
		 */
		public static class MapOutput implements MapFunction<Tuple4<Double, Long, Long, Long>, Tuple6<String, Double, Long, Long, Long, String>> {


			public Tuple6<String, Double, Long, Long, Long, String> map(Tuple4<Double, Long, Long, Long> t) throws Exception {


				return new Tuple6<String, Double, Long, Long, Long, String>(",", t.f0, t.f1, t.f2, t.f3, ",");

			}
		}


		public static class SumAllValues implements ReduceFunction<Tuple4<Integer, Long, Long, Long>> {
			@Override
			public Tuple4<Integer, Long, Long, Long> reduce(Tuple4<Integer, Long, Long, Long> value1, Tuple4<Integer, Long, Long, Long> value2) throws Exception {
				Long time = value1.f3;
				if (value1.f3 < value2.f3) {
					time = value2.f3;
				}
				return new Tuple4<Integer, Long, Long, Long>(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2, time);
			}
		}


		/**
		 * Returns the average number of passengers in a specific time window
		 */
		public static class PassengerCounter implements WindowFunction<
			Tuple3<Integer, Integer, Long>, // input type
			Tuple3<Double, String, Long>, // output type
			Tuple, // key type
			TimeWindow> // window type
		{

			@SuppressWarnings("unchecked")
			@Override
			public void apply(
				Tuple key,
				TimeWindow window,
				Iterable<Tuple3<Integer, Integer, Long>> values,
				Collector<Tuple3<Double, String, Long>> out) throws Exception {

//            Long cellId = ((Tuple2<Long, Integer>)key).f0;
				//           Integer passenger = ((Tuple2<Long, Integer>)key).f1;
				long windowTime = window.getStart();
				String time = new Date(window.getStart()).toString() + " " + new Date(window.getEnd()).toString() + " " + new Date(window.maxTimestamp()).toString();
				String dateString = new Date(windowTime).toString();
				Double cnt = 0.0;
				Double sum = 0.0;

				for (Tuple3<Integer, Integer, Long> v : values) {
					cnt += 1;
					sum += v.f1;
				}

				Date timeStamp = new Date(System.currentTimeMillis());
				Long duration = Long.valueOf(System.currentTimeMillis() - window.maxTimestamp());
				out.collect(new Tuple3<>(Double.valueOf(Math.round(sum / cnt * 1000.0) / 1000.0), String.valueOf(cnt), duration));
				//out.collect(new Tuple1<>( Double.valueOf( Math.round(cnt*100.0)/100.0)));
			}
		}

	}


