package com.example.triggerStreamTest.topology;

import com.example.triggerStreamTest.model.SourceEvent;
import com.example.triggerStreamTest.serde.JsonSerde;
import com.example.triggerStreamTest.service.EventService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

@AllArgsConstructor
@Slf4j
public class TriggerTopology {

    public static final String EVENTS_TOPIC = "events";

    private final EventService eventService;

    public Topology buildTopology(){
        JsonSerde<SourceEvent> eventJsonSerde = new JsonSerde<>(SourceEvent.class);

        StreamsBuilder sb = new StreamsBuilder();

        sb.stream(EVENTS_TOPIC,
                Consumed.with(Serdes.String(), eventJsonSerde))
                .foreach(((key, value) ->
                        eventService.routing(value)));

        return sb.build();
    }
}
