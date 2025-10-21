package com.example.triggerStreamTest.producer;

import com.example.triggerStreamTest.model.SourceEvent;
import com.example.triggerStreamTest.topology.TriggerTopology;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class EventProducer {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @SneakyThrows
    public static void main(String[] args) {
        Map<String, Object> props = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        var triggerProducer = new KafkaProducer<>(props, Serdes.String().serializer(), new JsonSerde<>(SourceEvent.class).serializer());

        Stream.of(OBJECT_MAPPER.readValue(EventProducer.class
                .getClassLoader().getResourceAsStream("data/test-data.json"),
                SourceEvent[].class))
                .map(event -> new ProducerRecord<>(TriggerTopology.EVENTS_TOPIC, event.getId(), event))
                .map(triggerProducer::send)
                .forEach(EventProducer::waitForProducer);
    }

    @SneakyThrows
    private static void waitForProducer(Future<RecordMetadata> recordMetadataFuture) {
        recordMetadataFuture.get();

    }
}
