package org.apache.flink.streaming.examples.kafka.tu.berlin.windowing;

/**
 * Created by andresviv on 26.02.17.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.util.SideInput;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.examples.kafka.tu.berlin.io.TaxiRideClass;
import org.apache.flink.streaming.examples.kafka.tu.berlin.serialization.FlinkSimpleStringTsSchema;
import org.apache.flink.util.Collector;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Read Strings from Kafka and print them to standard out.
 * Note: On a cluster, DataStream.print() will print to the TaskManager's .out file!
 *
 * Please pass the following arguments to run the example:
 * 	--topic test --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myconsumer
 *
 *
 * 	--topic spark-winagg5 --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id winagg5
 *
 * "/share/hadoop/kafka/kafkaAV/out/windowflink/latency
 *
 * arguments
 *
 * 5000
 */
public class FlinkHybridCluster1 {

	private static final String LOCAL_ZOOKEEPER_HOST = "ibm-power-1.dima.tu-berlin.de:2181";
	private static final String LOCAL_KAFKA_BROKER = "ibm-power-1.dima.tu-berlin.de:9092";
	private static final String RIDE_SPEED_GROUP = "winagg5";
	private static final int MAX_EVENT_DELAY = 60; // rides are at most 60 sec out-of-order.

	public static void main(String[] args) throws Exception {

		final String id= new BigInteger(130,new SecureRandom()).toString(32);

		/**
		 * *****************************************************************************************************************************
		 * *************************************Set up System ****************************************************************
		 * *****************************************************************************************************************************
		 */

	// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	//	env.setParallelism(1);
		env.getConfig().setAutoWatermarkInterval(1000);

		/**
		 * *****************************************************************************************************************************
		 * *************************************Read From Historical Data ****************************************************************
		 * *****************************************************************************************************************************
		 */
		//"/home/andresviv/temp/nyc200000.txt"
		String datah = args[1];
	//Read from Historical Data
		DataStream<String> historicalData = env.readTextFile(datah);
	//Mapping of the ID
		DataStream<Long> historicalID = historicalData.map(new Aggregations.MapHistoricalDatatoID3());
	//Creation of the SideInput
		final SideInput<Long> sideInputHistoricalData = env.newBroadcastedSideInput(historicalID);

		/**
		 * *****************************************************************************************************************************
		 * *************************************Read From Kafka (Data Streams)***********************************************************
		 * *****************************************************************************************************************************
		 */
		// configure the Kafka consumer
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
		kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
		kafkaProps.setProperty("group.id", RIDE_SPEED_GROUP);
		// always read the Kafka topic from the start
		kafkaProps.setProperty("auto.offset.reset", "earliest"); //earliest OK , latest
		kafkaProps.setProperty("enable.auto.commit", "true");

	// create a Kafka consumer
			FlinkKafkaConsumer08<String> consumer = new FlinkKafkaConsumer08<>(
			"spark-winagg5",
			new FlinkSimpleStringTsSchema(), kafkaProps);

	// create a TaxiRide data stream
		DataStream<String> rides = env.addSource(consumer);
		//DataStream<String>	windowrides = rides.keyBy(0).timeWindow(Time.milliseconds(5000));

		//DataStream<String> timeStampTuple = rides.map(x -> x + "," + System.currentTimeMillis());

			//	DataStream<Tuple3<Long,String,Long>> timeStampTuple = rides.map(new Aggregations.MapTimeStampStreaming2());
			//	DataStream<Tuple2<String, Long>> test = timeStampTuple.map(new MaptoID1());
			//	DataStream<String> test2 = timeStampTuple.map(new Tuple2<>(t,1));
			//	DataStream<Tuple3<String, Long, Long>> test2 = timeStampTuple.map(new MaptoID2());
			//DataStream<String> timeStampTuple2 = timeStampTuple.map(new Aggregations.MapTupletoString());

		/**
		 * *****************************************************************************************************************************
		 * *************************************     Mixing     ***********************************************************
		 * *****************************************************************************************************************************
		 */


/**
		//OK Mixing it
		DataStream<String> resultSideInputandKafkaStream =
			rides.rebalance()
				.map(new RichMapFunction<String, String>() {
					@Override
					public String map(String line) throws Exception {
						ArrayList<Long> side = (ArrayList<Long>) getRuntimeContext().getSideInput(sideInputHistoricalData);
						TaxiRideClass taxi = TaxiRideClass.fromString(line);
						for (Long s: side) {
							if (taxi.rideId == s) {
						//		line = new String(taxi.toString() + "," + s +getRuntimeContext().getTaskNameWithSubtasks()+","+System.currentTimeMillis()) ;
								line = new String(taxi.toString()) ;

							}
						}
					//	System.out.println("runtime: " + getRuntimeContext().getTaskNameWithSubtasks());
						//	DataStream<Long> side = (DataStream<Long>) getRuntimeContext().getSideInput(sideInput1);
						//	System.out.println("SEEING MAIN INPUT: " + value + " on " + getRuntimeContext().getTaskNameWithSubtasks() + " with " + side.size());
						//	return new String(taxi.toString()+"hola" + taxi.rideId);
						//	String val = line;
						return line;
					}
				})
				.withSideInput(sideInputHistoricalData);
*/
		//resultSideInputandKafkaStream.print();

		//DataStream<Long> fin = result.map(new Aggregations.MapHistoricalDatatoID3());
		//fin.print();
		/**
		DataStream<Tuple10<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long>> taxiside
						= resultSideInputandKafkaStream
			.map(new Aggregations.MapTaxitoTuple10())

			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple10<Long, Boolean, String, String, Float, Float, Float, Float, Short, Long>>() {
				@Override
				public long extractAscendingTimestamp(Tuple10<Long, Boolean, String, String, Float, Float, Float, Float, Short, Long> element) {
					return element.f9;
				}
			}).keyBy(value -> value.f0).timeWindow(Time.milliseconds(5000));

*/
/**
		DataStream<Tuple10<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long>> taxiside
			= resultSideInputandKafkaStream
			.map(new Aggregations.MapTaxitoTuple10())
			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple10<Long, Boolean, String, String, Float, Float, Float, Float, Short, Long>>() {
				@Override
				public long extractAscendingTimestamp(Tuple10<Long, Boolean, String, String, Float, Float, Float, Float, Short, Long> element) {
					return element.f9;
				}
			}).keyBy(0)
			.timeWindow(Time.milliseconds(2000)).sum(9);
*/
/**
//Si vale
		DataStream<Tuple10<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long>> taxiside
			= resultSideInputandKafkaStream
			.map(new Aggregations.MapTaxitoTuple10())
			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple10<Long, Boolean, String, String, Float, Float, Float, Float, Short, Long>>() {
				@Override
				public long extractAscendingTimestamp(Tuple10<Long, Boolean, String, String, Float, Float, Float, Float, Short, Long> element) {
					return element.f9;
				}
			}).keyBy(0)
			//.timeWindow(Time.milliseconds(2000)).maxBy(9);
;
 */
/**

				//si vale ok todo posi :)
		DataStream<Tuple4<Double, Long, Long,Long>> taxiside = resultSideInputandKafkaStream
		//rides
			.map(new Aggregations.MapPassenger())
			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, Long, Long, Long>>() {
				@Override
				public long extractAscendingTimestamp(Tuple4<Integer, Long, Long, Long> element) {
					return element.f3;
				}
			})
		//grouping all values
			.keyBy(0)
			// sliding window
			.timeWindow(Time.milliseconds(500))
			//sums the 1s and the passengers for the whole window
			.reduce(new Aggregations.SumAllValues())
			//.reduce(new Aggregations.SumAllValues(),new Aggregations.TSExtractor())
			.map(new Aggregations.MapToMean())

			;
*/

/**
			//SI Vale

		DataStream<Tuple15<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long,Double,Long, Long, Long, Long>>
			taxiside = resultSideInputandKafkaStream
			//rides
			.map(new Aggregations.MapPassengerH())
			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple15<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>>() {
				@Override
				public long extractAscendingTimestamp(Tuple15<Long,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long> element) {
					return element.f13;
				}
			})
			//grouping all values
			.keyBy(10)
			// sliding window
			.timeWindow(Time.milliseconds(500))
			//sums the 1s and the passengers for the whole window
			.reduce(new Aggregations.SumAllValuesH())
			//.reduce(new Aggregations.SumAllValues(),new Aggregations.TSExtractor())
			.map(new Aggregations.MapToMeanH())

			;
*/
		DataStream<Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>>
			taxiside = rides
			//rides
			.map(new Aggregations.MapPassengerR())

			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>>() {
				@Override
				public long extractAscendingTimestamp(Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long> element) {
					return element.f9;
				}
			})
			//grouping all values
			.keyBy(0)
			// sliding window
			.timeWindow(Time.milliseconds(Long.valueOf(args[0])))
			//sums the 1s and the passengers for the whole window
			// Patrick did not reduce, delete
			.reduce(new Aggregations.SumAllValuesR())
			//.reduce(new Aggregations.SumAllValues(),new Aggregations.TSExtractor())
			.map(new RichMapFunction<Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>,
				Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>>() {
				@Override
				public Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>
				map(Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long> t)
					throws Exception {
					ArrayList<Long> side = (ArrayList<Long>) getRuntimeContext().getSideInput(sideInputHistoricalData);
					//TaxiRideClass taxi = TaxiRideClass.fromString(line);
					Long millis = System.currentTimeMillis();
					Long duration = millis - t.f12;
					Long seconds = duration/1000;
					Long idrides = t.f0.longValue();
					for (Long s: side) {
										//	Long idrides = t.f0.longValue();
						if (s == idrides) {
							//		line = new String(taxi.toString() + "," + s +getRuntimeContext().getTaskNameWithSubtasks()+","+System.currentTimeMillis()) ;
							t = new Tuple15<Integer,Boolean,String,String,Float,Float,Float,Float,Short,Long,Integer,Long, Long, Long, Long>
						//		(t.f0,t.f1,t.f2,t.f3,t.f4,t.f5,t.f6,t.f7,t.f8,t.f9,(Math.round(t.f12 / new Double(t.f11) * 1000) / 1000.0), t.f11, millis,seconds,duration);
//							(t.f0,t.f1,t.f2,t.f3,t.f4,t.f5,t.f6,t.f7,t.f8,t.f9,t.f10,
//								t.f11, millis,seconds,(System.currentTimeMillis()-t.f12));
							(t.f0,t.f1,t.f2,t.f3,t.f4,t.f5,t.f6,t.f7,t.f8,t.f9,t.f10,
								t.f11, t.f12,(System.currentTimeMillis()-t.f12)/1000,
								(System.currentTimeMillis()-t.f12));

						}
					}
					//	System.out.println("runtime: " + getRuntimeContext().getTaskNameWithSubtasks());
					//	DataStream<Long> side = (DataStream<Long>) getRuntimeContext().getSideInput(sideInput1);
					//	System.out.println("SEEING MAIN INPUT: " + value + " on " + getRuntimeContext().getTaskNameWithSubtasks() + " with " + side.size());
					//	return new String(taxi.toString()+"hola" + taxi.rideId);
					//	String val = line;
					return t;
				}
			})
			.withSideInput(sideInputHistoricalData);


		taxiside.print();


		taxiside.writeAsText("/share/hadoop/proteusengineandresviv/out/fin.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

//   "/share/hadoop/kafka/kafkaAV/out/windowflink/latency")
		//	rides.print();

		// execute the transformation pipeline
		env.execute("Flink Hybrid Benchmarking");

	}




	/**
	 * *****************************************************************************************************************************
	 * *************************************Some Functions***********************************************************
	 * *****************************************************************************************************************************
	 */

	public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		@Override
		public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
			for (String word : line.split(" ")) {
				out.collect(new Tuple2<String, Integer>(word, 1));
			}
		}
	}


	private static class MaptoID1 extends RichMapFunction<String, Tuple2<String, Long>> {
		private static final long serialVersionUID = 1L;
		@Override
		public Tuple2<String, Long> map(String record) {
			String[] data = record.split(",");
			return new Tuple2<>(String.valueOf(data[0]), Long.valueOf(data[0]));
		}
	}


	private static class MaptoID2 extends RichMapFunction<String, Tuple3<String, Long, Long>> {
		private static final long serialVersionUID = 1L;
		@Override
		public Tuple3<String, Long, Long> map(String record) {
			String[] data = record.split(",");
			return new Tuple3<>(String.valueOf(data[0]), Long.valueOf(data[0]), System.currentTimeMillis());
		}
	}

	public static class MapSide extends RichMapFunction<String, Tuple3<Long, String, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<Long,String, Long> map(String line) throws Exception {
		//	ArrayList<Long> side = (ArrayList<Long>) getRuntimeContext().getSideInput(sideInput1);
		//	System.out.println("SEEING MAIN INPUT: " + " on " + getRuntimeContext().getTaskNameWithSubtasks() + " with " + side.size());
			TaxiRideClass taxi = TaxiRideClass.fromString(line);
			return new Tuple3<>(taxi.rideId,taxi.toString(), System.currentTimeMillis());
		}


	}


}
