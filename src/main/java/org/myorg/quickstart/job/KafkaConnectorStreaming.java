package org.myorg.quickstart.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

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

}
