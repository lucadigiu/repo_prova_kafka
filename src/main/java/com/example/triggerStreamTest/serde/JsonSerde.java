package com.example.triggerStreamTest.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerde<T> implements Serde<T> {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Class<T> type;

    public JsonSerde(Class<T> type) {
        this.type=type;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> getBytes(data);
    }

    @SneakyThrows
    private static <T> byte[] getBytes(T data) {
        return OBJECT_MAPPER.writeValueAsBytes(data);
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, data) -> getReadValue(data);
    }

    @SneakyThrows
    private T getReadValue(byte[] data) {
        return OBJECT_MAPPER.readValue(data, type);
    }
}
