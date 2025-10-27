package com.example.triggerStreamTest.launcher;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import java.util.Properties;

@Slf4j
public class KafkaYellingApp {

    public static Topology getTopology(Properties streamProperties) {
        StreamsBuilder sb = new StreamsBuilder();

        KStream<String, String> firstStream = sb.stream("src-topic",
                        Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues((onlyReadKey, value) -> value.toUpperCase());

        firstStream.to("output-topic",
                Produced.with(Serdes.String(), Serdes.String()));

        return sb.build(streamProperties);
    }

    public static void main(String[] args) {
        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "first-stream-app");
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        streamProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Topology topology = getTopology(streamProperties);


        KafkaStreams ks = new KafkaStreams(topology, streamProperties);

        Runtime.getRuntime().addShutdownHook(new Thread(ks::close));

        try {
            log.info("Hello World Yelling App Started");
            ks.start();
        } catch (Exception e) {
            log.error("Error starting Kafka Streams", e);
        }
    }
}
