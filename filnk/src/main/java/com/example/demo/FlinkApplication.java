package com.example.demo;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class FlinkApplication {

  static String ZK_HOST = "101.200.137.83:2181";
  static String KAFKA_HOST = "101.200.137.83:9092";
  static String C_GROUP = "cus";


  public static void main(String[] args) throws Exception {
    SpringApplication.run(FlinkApplication.class, args);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

    Properties kafkaProps = new Properties();
    kafkaProps.setProperty("zookeeper.connect", ZK_HOST);
    kafkaProps.setProperty("bootstrap.servers", KAFKA_HOST);
    kafkaProps.setProperty("group.id", C_GROUP);

    FlinkKafkaConsumer flinkKafkaConsumer =
        new FlinkKafkaConsumer<String>("Shakespeare", new SimpleStringSchema(), kafkaProps);
    flinkKafkaConsumer.setStartFromTimestamp(1595332219065L);

    DataStream<String> data = env.addSource(flinkKafkaConsumer);

    DataStream<Message> msgSource = data.map(new MapFunction<String, Message>() {
      @Override
      public Message map(String value) throws Exception {
        try {
          return new Gson().fromJson(value, Message.class);
        } catch (Exception e) {
          Message message = new Message();
          message.setMsg(value);
          return message;
        }
      }
    });

    //统计每个uid 出现的次数
    msgSource.flatMap(new FlatMapFunction<Message, Tuple2<String, Integer>>() {
      @Override
      public void flatMap(Message value, Collector<Tuple2<String, Integer>> out) throws Exception {
        out.collect(new Tuple2<>(value.getMsg(), 1));
      }
    })
        .keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
          @Override
          public Object getKey(Tuple2<String, Integer> value) throws Exception {
            return value.f0;
          }
        })
        .timeWindow(Time.milliseconds(30))
        .sum(1)
        .addSink(new SinkFunction<Tuple2<String, Integer>>() {
          @Override
          public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            System.out.println(System.currentTimeMillis() + "----" +value);
          }
        });

    env.execute("load-data");
  }
}
