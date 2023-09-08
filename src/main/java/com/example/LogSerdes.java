package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

public class LogSerdes implements Serde<Log> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Serializer<Log> serializer() {
        return (topic, data) -> new byte[0];
    }

    @Override
    public Deserializer<Log> deserializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.readValue(data, Log.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
