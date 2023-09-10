package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class RealtimePostAnalyzer {
    private static final LogSerdes logSerdes = new LogSerdes();
    private final static Logger logger = LoggerFactory.getLogger(RealtimePostAnalyzer.class);
    private static final String APP_ID = "realtime-post-analyzer";
    private static final String BOOTSTRAP_SERVER = "my-kafka:9092";
    private static final String MYSQL_LOG_TOPIC = "log.alert_dangerous_posts.posts";
    private static final String POSTS = "posts";
    private static final String HOURLY_POSTS_TOPIC = "hourly_posts";
    private static final String DANGEROUS_POSTS_TOPIC = "dangerous_posts";

    private static Set<String> dangerousKeyWords = Set.of(
            "행복","미소","사랑","배려","미래","희망","용기","가능성","자신감","긍정"
    );

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, logSerdes.getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Log> hourlyPostsCounter = createHourlyPostsCounter(builder);
        KStream<String, Log> dangerousPostsFilter = createDangerousPostsFilter(builder);
        KStream<String, Log> logToPostsMapper = createLogToPostsMapper(builder);

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KStream<String, Log> createLogToPostsMapper(StreamsBuilder builder) {
        KStream<String, Log> logToPostsMapper = builder.stream(MYSQL_LOG_TOPIC, Consumed.with(Serdes.String(), logSerdes));

        logToPostsMapper
                .filter((key, value) -> "c".equals(value.getPayload().get("__op")))
                .map((KeyValue::new))
                .to(POSTS);

        return logToPostsMapper;
    }

    private static KStream<String, Log> createDangerousPostsFilter(StreamsBuilder builder) {
        KStream<String, Log> dangerousPostsFilter = builder.stream(POSTS, Consumed.with(Serdes.String(), logSerdes));

        dangerousPostsFilter
                .filter(((key, value) -> isDangerous(value.toString())))
                .map((key, value) -> new KeyValue<>("POST", value.toString()))
                .to(DANGEROUS_POSTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return dangerousPostsFilter;
    }

    private static boolean isDangerous(String message) {
        logger.debug(message);
        for (String word : dangerousKeyWords) {
            if (message.contains(word)) {
                return true;
            }
        }
        return false;
    }

    private static KStream<String, Log> createHourlyPostsCounter(StreamsBuilder builder) {
        KStream<String, Log> hourlyPostsCounter = builder.stream(MYSQL_LOG_TOPIC, Consumed.with(Serdes.String(), logSerdes));

        hourlyPostsCounter
                .selectKey(((key, value) -> value.getPayload().get("__op")))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
                .count()
                .toStream()
                .foreach((key, value) -> logger.info("[" + key.window().startTime() + "~" + key.window().endTime() + "]" + " = " + value));
//                .map((KeyValue<String, Long> kv, Long v) -> new KeyValue<>(kv.key, v.toString()))
//                .to(HOURLY_POSTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return hourlyPostsCounter;
    }
}
