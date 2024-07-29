package example;

/**
 * Hello world!
 */

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.functions.KeySelector;
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
import scala.Tuple2;

import java.net.URI;
import java.text.ParseException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
//import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.api.TimeCharacteristic.EventTime;

public class App {

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

    public static void main(String[] args) throws Exception {
        // create stream environment
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(EventTime);

        // define watermark strategy for TaxiRide data point = element.getPickupDatetime().getTime());
        WatermarkStrategy<TaxiRide> orderWatermarkStrategy = WatermarkStrategy
                .<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((element, timestamp) ->
                        element.getPickupDatetime().getTime());

        // read yellow taxi data from CSV file
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
                });


        // read green Taxi data from CSV file
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
                });

        // 将模式应用到数据流
        // combine both streams for patterns, assign the timestamp and the key (for condition vendorid == vendorid
        DataStream<TaxiRide> allTaxiRides = yellowTaxiRides.union(greenTaxiRides)
                        .assignTimestampsAndWatermarks(orderWatermarkStrategy)
                .keyBy(new KeySelector<TaxiRide, Double>() {
                    @Override
                    public Double getKey(TaxiRide taxiRide) throws Exception {
                        return taxiRide.getVendorID();
                    }
                });;

        // Pattern: triggering a CE when yellow and green taxis with vendorID 1 appear simultaneously within a ten-minute window
        // The pattern is a bit fuzy please write in PSL else natural language is tricky; I understood:
        /** PATTERN AND(Yellow, Green)
         * WHERE id = id AND id = 1.0
         * WINDOW 10 minutes
         */
        // Flink has no and only SEQ thus you need SEQ (Yellow, GReen) and SEQ (Green, Yellow), id = id is automatically handled by keyby for key = 1.0 you need a filter condition
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.noSkip();
        // pattern1: SEQ (Yellow, Green)
        Pattern<TaxiRide, ?> pattern1 = Pattern.<TaxiRide>begin("start", skipStrategy)
                .where(new SimpleCondition<TaxiRide>(){

                    @Override
                    public boolean filter(TaxiRide taxiRide) throws Exception {
                        if (taxiRide.getColor().equals("yellow") && taxiRide.getVendorID() == 1.0){
                            return true;
                        }
                        return false;
                    }
                }).followedByAny("followedBy")
                .where(new SimpleCondition<TaxiRide>(){

                    @Override
                    public boolean filter(TaxiRide taxiRide) throws Exception {
                        if (taxiRide.getColor().equals("green") && taxiRide.getVendorID() == 1.0 ){
                            return true;
                        }
                        return false;
                    }
                }).within(Time.minutes(10));

        // pattern2: SEQ (Green, Yellow)
        Pattern<TaxiRide, ?> pattern2 = Pattern.<TaxiRide>begin("start", skipStrategy)
                .where(new SimpleCondition<TaxiRide>(){

                    @Override
                    public boolean filter(TaxiRide taxiRide) throws Exception {
                        if (taxiRide.getColor().equals("green") && taxiRide.getVendorID() == 1.0){
                            return true;
                        }
                        return false;
                    }
                }).followedByAny("followedBy")
                .where(new SimpleCondition<TaxiRide>(){

                    @Override
                    public boolean filter(TaxiRide taxiRide) throws Exception {
                        if (taxiRide.getColor().equals("yellow") && taxiRide.getVendorID() == 1.0){
                            return true;
                        }
                        return false;
                    }
                }).within(Time.minutes(10));

        // 将模式应用到数据流上
        PatternStream<TaxiRide> patternStreamYellowGreen = CEP.pattern(allTaxiRides, pattern1);
        PatternStream<TaxiRide> patternStreamGreenYellow = CEP.pattern(allTaxiRides, pattern2);


        // 从匹配的模式中选择结果
        DataStream<Tuple2<TaxiRide,TaxiRide>> resultYellowGreen = patternStreamYellowGreen.process(new PatternProcessFunction<TaxiRide, Tuple2<TaxiRide,TaxiRide>>() {
            @Override
            public void processMatch(Map<String, List<TaxiRide>> match, Context context, Collector<Tuple2<TaxiRide, TaxiRide>> collector) throws Exception {
                TaxiRide StartRide = match.get("start").get(0);
                TaxiRide EndRide = match.get("followedBy").get(0);
                collector.collect(new Tuple2<TaxiRide,TaxiRide>(StartRide,EndRide));
            }
        });

        DataStream<Tuple2<TaxiRide,TaxiRide>> resultGreenYellow = patternStreamGreenYellow.process(new PatternProcessFunction<TaxiRide, Tuple2<TaxiRide,TaxiRide>>() {
            @Override
            public void processMatch(Map<String, List<TaxiRide>> match, Context context, Collector<Tuple2<TaxiRide, TaxiRide>> collector) throws Exception {
                TaxiRide StartRide = match.get("start").get(0);
                TaxiRide EndRide = match.get("followedBy").get(0);
                collector.collect(new Tuple2<TaxiRide,TaxiRide>(StartRide,EndRide));
            }
        });

        DataStream<Tuple2<TaxiRide,TaxiRide>> result = resultGreenYellow.union(resultYellowGreen);
        String outputPath = "taxi_result/result.csv";
        result.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        // this works for me the results look good to me as well
        // 执行Flink作业
        env.execute("Taxi Ride CEP Example");
    }

}
