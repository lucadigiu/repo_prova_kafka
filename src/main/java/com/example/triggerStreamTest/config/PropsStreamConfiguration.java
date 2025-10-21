package com.example.triggerStreamTest.config;

import com.example.triggerStreamTest.service.EventService;
import com.example.triggerStreamTest.topology.TriggerTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class PropsStreamConfiguration {

    @Bean
    public Properties createProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "trigger-stream-app");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    @Bean
    public TriggerTopology getTriggerTopology(EventService eventService) {
        return new TriggerTopology(eventService);
    }

    @Bean
    public Topology topology(TriggerTopology triggerTopology){
        return triggerTopology.buildTopology();
    }

    @Bean
    public KafkaStreams kafkaStreams(Topology topology, @Qualifier("createProps") Properties properties){
        KafkaStreams ks = new KafkaStreams(topology, properties);
        ks.start();
        Runtime.getRuntime().addShutdownHook(new Thread(ks::close));
        return ks;
    }
}
