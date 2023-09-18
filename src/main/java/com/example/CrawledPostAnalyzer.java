package com.example;

import com.example.filter.Filter;
import com.example.filter.KeywordMatchFilter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CrawledPostAnalyzer {
    private static Logger logger = LoggerFactory.getLogger(CrawledPostAnalyzer.class);
    private static final String APP_ID = "crawled-post-analyzer";
    private static final String BOOTSTRAP_SERVER = "kafka.lwu.me:9093";
    private static final String POSTS = "posts";
    private static final String DANGEROUS_POSTS = "dangerous_posts";
    private static final Filter dangerousPostFilter = new KeywordMatchFilter();

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream = builder.stream(POSTS);

        stream
                .filter((key, value) -> dangerousPostFilter.filter(value))
                .to(DANGEROUS_POSTS);

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }
}
