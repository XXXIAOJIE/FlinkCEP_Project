package example;

/**
 * Hello world!
 *
 */
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URI;
import java.text.ParseException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
//import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.api.TimeCharacteristic.EventTime;

public class App {

    public static void main(String[] args) throws Exception {
        DataStream<String> result1 = Taxi();
       // DataStream<String> result2 = Sensor();
//        DataStream<Tuple2<KeyedDataPointGeneral, KeyedDataPointGeneral>> result = patternStream.flatSelect(new UDFs.GetResultTuple2());
//        result.flatMap(new LatencyLoggerT2(true));
//        result.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);
    }

    public static class URISerializer extends Serializer<URI> {
        @Override
        public void write(Kryo kryo, Output output, URI uri) {
            output.writeString(uri.toString());
        }
        @Override
        public URI read(Kryo kryo, Input input, Class<URI> aClass) {
            return URI.create(input.readString());
        }
    }


    static DataStream<String> Taxi() throws Exception {
        //BasicConfigurator.configure();
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(EventTime);

        // 注册自定义Kryo序列化器
        env.getConfig().registerTypeWithKryoSerializer(URI.class, URISerializer.class);

        /** WatermarkStrategy<TaxiRide> orderWatermarkStrategy = CustomWatermarkStrategy
               * .<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                 .withTimestampAssigner((element, timestamp) ->
                         element.getPickupDatetime(); // TODO that needs to be milliseconds not Data
                 );*/

        WatermarkStrategy<TaxiRide> orderWatermarkStrategy = WatermarkStrategy
                .<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((element, timestamp) ->
                        element.getPickupDatetime().getTime());


        // read yellow taxi data
        DataStream<TaxiRide> yellowTaxiRides = env.readTextFile("taxi_data/yellow_tripdata_2020-05(5am-8am).csv")
                .flatMap(new FlatMapFunction<String, TaxiRide>() {
                    private boolean isFirstLine = true;
                    @Override
                    public void flatMap(String s, Collector<TaxiRide> collector) throws ParseException {
                        if (isFirstLine) {
                            isFirstLine = false;
                        } else {
                            String[] fields = s.split(";");
                            TaxiRide ride = new TaxiRide("yellow", Double.parseDouble(fields[0]), fields[1], fields[2],
                                    Double.parseDouble(fields[8]), Double.parseDouble(fields[16]), 0);
                            collector.collect(ride);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(orderWatermarkStrategy);


        // read green Taxi data
        DataStream<TaxiRide> greenTaxiRides = env.readTextFile("taxi_data/green_tripdata_2020-05(5am-8am).csv")
                .flatMap(new FlatMapFunction<String, TaxiRide>() {
                    private boolean isFirstLine = true;
                    @Override
                    public void flatMap(String s, Collector<TaxiRide> collector) throws ParseException {
                        if (isFirstLine) {
                            isFirstLine = false;
                        } else {
                            String[] fields = s.split(";");
                            TaxiRide ride = new TaxiRide("green", Double.parseDouble(fields[0]), fields[1], fields[2],
                                    Double.parseDouble(fields[8]), Double.parseDouble(fields[16]), 0);
                            collector.collect(ride);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(orderWatermarkStrategy);

        // 将模式应用到数据流
        DataStream<TaxiRide> allTaxiRides = yellowTaxiRides.union(greenTaxiRides);

        // Pattern: triggering a CE when yellow and green taxis with vendorID 1 appear simultaneously within a ten-minute window
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.noSkip();
        Pattern<TaxiRide, ?> pattern = Pattern.<TaxiRide>begin("start", skipStrategy)
                .where(new IterativeCondition<TaxiRide>() {
                    @Override
                    public boolean filter(TaxiRide ride, Context<TaxiRide> ctx) throws Exception {
                        return (ride.getColor().equals("yellow") && ride.getVendorID() == 1 || ride.getColor().equals("green")) && ride.getVendorID() == 1;
                    }
                })
                .times(2)  // 期待两个匹配事件
//                .where(new SimpleCondition<TaxiRide>() {
//                    @Override
//                    public boolean filter(TaxiRide ride) {
//                        return ride.getColor().equals("yellow") && ride.getVendorID() == 1;
//                    }
//                })
//                .where(new SimpleCondition<TaxiRide>() {
//                    @Override
//                    public boolean filter(TaxiRide ride) {
//                        return ride.getColor().equals("green") && ride.getVendorID() == 1;
//                    }
//                })
                .within(Time.minutes(10));

        // 将模式应用到数据流上
        //PatternStream<TaxiRide> patternStream = CEP.pattern(allTaxiRides.keyBy(TaxiRide::getColor), pattern);
        PatternStream<TaxiRide> patternStream = CEP.pattern(allTaxiRides, pattern);


        // 从匹配的模式中选择结果
        DataStream<String> result = patternStream.process(new PatternProcessFunction<TaxiRide, String>() {
            @Override//Called every time a matching sequence of events is found.
            public void processMatch(Map<String, List<TaxiRide>> match, Context context, Collector<String> collector) throws Exception {
                StringBuilder matches = new StringBuilder();
                List<TaxiRide> rides = match.get("start");
                TaxiRide yellowRide = rides.get(0);
                TaxiRide greenRide = rides.get(1);

                String s = "Yellow taxi pickup at_" + yellowRide.getPickupDatetime() + "_drop at_" + yellowRide.getDropoffDatetime() +
                        "_with Green taxi at_" + greenRide.getPickupDatetime() + "_drop at_" + greenRide.getDropoffDatetime();
                collector.collect(s);
            }
        });

        result.print();
        String outputPath = "taxi_result/result1.csv";
        result.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);
        //create csv datei
//        final StreamingFileSink<String> sink = StreamingFileSink
//                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
//                .withRollingPolicy(DefaultRollingPolicy.builder()
//                        .withRolloverInterval(Duration.ofMinutes(1_000).toMillis())
//                        .withInactivityInterval(Duration.ofMinutes(1_000).toMillis())
//                        .withMaxPartSize(1024 * 1024 * 1024)
//                        .build())
//                .build();
//
//        result.addSink(sink);

        // 执行Flink作业
        env.execute("Taxi Ride CEP Example");
        return result;
    }


    private static DataStream<String> Sensor() {
        DataStream<String> result = null;
        return result;
    }


}
