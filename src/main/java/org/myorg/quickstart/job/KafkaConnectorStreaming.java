package org.myorg.quickstart.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.model.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class KafkaConnectorStreaming {
    String topicNameInput;
    String topicNameOutput;
    String topicNameSource2;

    public KafkaConnectorStreaming(String topicNameInput, String source2TopicName, String topicNameOutput) {
        this.topicNameInput = topicNameInput;
        this.topicNameOutput = topicNameOutput;
        this.topicNameSource2 = source2TopicName;
    }

    //		bootstrap servers URL: localhost:9092



     public void kafkaConnectorLearning(StreamExecutionEnvironment env) {
//        For docker this should be kafka:9093 for local flink it will be localhost:9092
         String bootstrapServers =  "kafka:9093";
         Logger logger= LoggerFactory.getLogger(KafkaConnectorStreaming.class);
         KafkaSource<String> source = KafkaSource.<String>builder()
                 .setBootstrapServers(bootstrapServers)
                 .setTopics(topicNameInput)
                 .setGroupId("my-group")
                 .setStartingOffsets(OffsetsInitializer.earliest())
                 .setValueOnlyDeserializer(new SimpleStringSchema())
                 .build();

         KafkaSource<String> source2 = KafkaSource.<String>builder()
                 .setBootstrapServers(bootstrapServers)
                 .setTopics(topicNameInput)
                 .setGroupId("my-group-source2")
                 .setStartingOffsets(OffsetsInitializer.earliest())
                 .setValueOnlyDeserializer(new SimpleStringSchema())
                 .build();

         KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
                 .setValueSerializationSchema(new SimpleStringSchema())
                 .setTopic(topicNameOutput)
                 .build();
//         KafkaSink<String> sink = KafkaSink.<String>builder()
//                 .setBootstrapServers(bootstrapServers)
//                 .setRecordSerializer(serializer)
////                 .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                 .build();

         DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source1");
         DataStream<String> stream2 = env.fromSource(source2, WatermarkStrategy.noWatermarks(), "Kafka Source2");

         stream.print();
         stream2.print();
         logger.info("this stream started: "+stream.print()+ " and second stream :"+stream2.print());

//         Need implements join here :
         DataStream<String> joinedStream = stream.join(stream2)
                 .where(order -> parseOrderId(order))
                 .equalTo(shipment -> parseOrderId(shipment))
                     .window(SlidingEventTimeWindows.of(Time.hours(4), Time.minutes(30)))
                 .apply(new JoinFunction<String, String, String>() {
                     @Override
                     public String join(String order, String shipment) {
                         return "Joined: " + order + " WITH " + shipment;
                     }
                 });
         joinedStream.print();
         logger.info("Joined Stream with Windowing Started"+joinedStream.print());

         stream.keyBy(order -> parseOrderId(order))
                 .intervalJoin(stream2.keyBy(shipment -> parseOrderId(shipment)))
                 .between(Time.minutes(0), Time.hours(4))
                 .process(new ProcessJoinFunction<String, String, String>() {
                     @Override
                     public void processElement(String order, String shipment, Context ctx, Collector<String> out) {
                         out.collect("Joined: " + order + " WITH " + shipment);
                     }
                 })
                 .print();

         logger.info("Kafka Connector Streaming Job Started"+joinedStream);
         // Split up the lines in pairs (2-tuples) containing: (word,1)
//         DataStream<String> counts = text.flatMap(new Tokenizer())
//                 // Group by the tuple field "0" and sum up tuple field "1"
//                 .keyBy(value -> value.f0)
//                 .sum(1)
//                 .flatMap(new Reducer());
////
////         // Add the sink to so results
////         // are written to the outputTopic
//         counts.sinkTo(sink);

     }
    private static String parseOrderId(String json) {
        // Dummy parser: Implement actual JSON extraction logic here
        Logger log = LoggerFactory.getLogger(KafkaConnectorStreaming.class);
        log.info("Parsing order ID from JSON: " + json);
        return json.split(",")[0];
    }

}
