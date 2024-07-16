package example;

/**
 * Hello world!
 *
 */
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.ParseException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.api.TimeCharacteristic.EventTime;

public class App {

    public static void main(String[] args) throws Exception {
        //System.out.println("hello");
        DataStream<String> result1 = Taxi();
        DataStream<String> result2 = Sensor();
    }


    private static DataStream<String> Taxi() throws Exception {
        //BasicConfigurator.configure();
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(EventTime);

       /** WatermarkStrategy<TaxiRide> orderWatermarkStrategy = CustomWatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((element, timestamp) ->
                        element.getPickupDatetime(); // TODO that needs to be milliseconds not Data
                );*/


        // 创建一个数据流
        DataStream<TaxiRide> yellowTaxiRides = env.readTextFile("/Users/xiaojiesun/tu/lunwen/prepare/yellow_tripdata_2024-01.csv")
                .map(new MapFunction<>() {
                    @Override
                    public TaxiRide map(String value) throws ParseException {
                        String[] fields = value.split(",");
                        return new TaxiRide("yellow", Double.parseDouble(fields[0]), fields[1], fields[2],
                                Double.parseDouble(fields[8]), Double.parseDouble(fields[16]), 0);
                    }
                }); //.assignTimestampsAndWatermarks(orderWatermarkStrategy);


        // 读取绿色出租车数据流
        DataStream<TaxiRide> greenTaxiRides = env.readTextFile("/Users/xiaojiesun/tu/lunwen/prepare/green_tripdata_2024-01.csv")
                .map(new MapFunction<String, TaxiRide>() {
                    @Override
                    public TaxiRide map(String value) throws ParseException {
                        String[] fields = value.split(",");
                        return new TaxiRide("green", Double.parseDouble(fields[0]), fields[1], fields[2],
                                Double.parseDouble(fields[8]), Double.parseDouble(fields[16]), 0);
                    }
                });

        // 将模式应用到数据流
        DataStream<TaxiRide> allTaxiRides = yellowTaxiRides.union(greenTaxiRides);

        // 定义一个模式
        Pattern<TaxiRide, ?> pattern = Pattern.<TaxiRide>begin("start")
                .where(new SimpleCondition<TaxiRide>() {
                    @Override
                    public boolean filter(TaxiRide ride) {
                        return ride.getColor().equals("yellow") && ride.getVendorID() == 1;
                    }
                })
                .where(new SimpleCondition<TaxiRide>() {
                    @Override
                    public boolean filter(TaxiRide ride) {
                        return ride.getColor().equals("green") && ride.getVendorID() == 1;
                    }
                })
                .within(Time.minutes(10));

        // 将模式应用到数据流上
        PatternStream<TaxiRide> patternStream = CEP.pattern(allTaxiRides.keyBy(TaxiRide::getVendorID), pattern);

        // 从匹配的模式中选择结果
        DataStream<String> result = patternStream.select(new PatternSelectFunction<TaxiRide, String>() {
            @Override
            public String select(Map<String, List<TaxiRide>> pattern) {
                TaxiRide yellowRide = pattern.get("yellow").get(0);
                TaxiRide greenRide = pattern.get("green").get(0);
                return "Match found! VendorID: " + yellowRide.getVendorID() +
                        ", Yellow Pickup: " + yellowRide.getPickupDatetime() +
                        ", Green Pickup: " + greenRide.getPickupDatetime();
            }
        });

        // 输出结果
        result.print();

        // 执行Flink作业
        env.execute("Taxi Ride CEP Example");
        return result;
    }


    private static DataStream<String> Sensor() {
        DataStream<String> result = null;
        return result;
    }


}
