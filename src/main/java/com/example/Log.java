package com.example;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class Log {
    private Object schema;
    private Map<String, Object> payload = new HashMap<>();
}