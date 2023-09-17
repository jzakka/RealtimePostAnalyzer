package com.example;

import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class CrawledPostAnalyzer {
    private static Logger logger = LoggerFactory.getLogger(CrawledPostAnalyzer.class);
    private static final String APP_ID = "crawled-post-analyzer";
    private static final String BOOTSTRAP_SERVER = "kafka.lwu.me:9093";
    private static final String POSTS = "posts";
    private static final String DANGEROUS_POSTS = "dangerous_posts";
    private static final String FILTER_PATH = "Filter";
    private static final String FILTER_NAME = "KeywordMatchFilter.py";

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream = builder.stream(POSTS);

        stream
                .filter((key, value) -> isDangerous(value))
                .to(DANGEROUS_POSTS);

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }

    private static boolean isDangerous(String value) {
        String filterPath = getFilterPath();
//        logger.info("python.path=" + filterPath);
        ProcessBuilder judgeBuilder = new ProcessBuilder("python", filterPath, value);

        try {
            Process judge = judgeBuilder.start();
            int exitCode = judge.waitFor();

            if (exitCode != 0) {
                throw new RuntimeException("게시글 분류기가 비정상 종료되었음");
            }

            String result = getResult(judge);
//            logger.info("python.result=" + result);

            return result.equals("dangerous");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private static String  getFilterPath() {
        URL resource = CrawledPostAnalyzer.class.getClassLoader().getResource(FILTER_PATH + "/" + FILTER_NAME);
        return resource.getFile();
    }

    @SneakyThrows
    private static String getResult(Process judge) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(judge.getInputStream(), StandardCharsets.UTF_8));
        return reader.readLine();
    }
}
