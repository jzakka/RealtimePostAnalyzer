package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LogSerdes implements Serde<Log> {
    private final static Logger logger = LoggerFactory.getLogger(LogSerdes.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Serializer<Log> serializer() {
        return (topic, data) -> {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Deserializer<Log> deserializer() {
        return (topic, data) -> {
            logger.debug(data.toString());
            if (data == null) {
                return null;
            }
            try {
                return objectMapper.readValue(data, Log.class);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        };
    }
}
