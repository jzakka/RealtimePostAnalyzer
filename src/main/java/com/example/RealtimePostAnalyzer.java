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
    private static final String MYSQL_LOG_TOPIC = "mysql-.alert_dangerous_posts.posts";
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
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> hourlyPostsCounter = createHourlyPostsCounter(builder);
        KStream<String, String> dangerousPostsFilter = createDangerousPostsFilter(builder);
        KStream<String, Log> logToPostsMapper = createLogToPostsMapper(builder);

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KStream<String, Log> createLogToPostsMapper(StreamsBuilder builder) {
        KStream<String, Log> logToPostsMapper = builder.stream(MYSQL_LOG_TOPIC, Consumed.with(Serdes.String(), logSerdes));

        logToPostsMapper
                .filter((key, value) -> "c".equals(value.getPayload().get("__op")))
                .map(((key, value) -> new KeyValue<>(key, value.toString())))
                .to(POSTS, Produced.with(Serdes.String(), Serdes.String()));

        return logToPostsMapper;
    }

    private static KStream<String, String> createDangerousPostsFilter(StreamsBuilder builder) {
        KStream<String, String> dangerousPostsFilter = builder.stream(POSTS);

        dangerousPostsFilter
                .filter(((key, value) -> isDangerous(value)))
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

    private static KStream<String, String> createHourlyPostsCounter(StreamsBuilder builder) {
        KStream<String, String> hourlyPostsCounter = builder.stream(MYSQL_LOG_TOPIC);

        hourlyPostsCounter
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
                .count()
                .toStream((Windowed<String> key, Long val) -> {
                    String start = dateFormat.format(new Date(key.window().start()));
                    String end = dateFormat.format(new Date(key.window().end()));
                    String newKey = start + " ~ " + end;
                    return KeyValue.pair(newKey, val);
                })
                .map((KeyValue<String, Long> kv, Long v) -> new KeyValue<>(kv.key, v.toString()))
                .to(HOURLY_POSTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return hourlyPostsCounter;
    }
}
