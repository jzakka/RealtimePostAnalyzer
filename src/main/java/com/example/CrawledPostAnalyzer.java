package com.example;

import com.example.filter.Filter;
import com.example.filter.KeywordMatchFilter;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Properties;

public class CrawledPostAnalyzer {
    private static Logger logger = LoggerFactory.getLogger(CrawledPostAnalyzer.class);
    private static final String APP_ID = "crawled-post-analyzer";
    private static final String BOOTSTRAP_SERVER = "kafka.lwu.me:9093";
    private static final String POSTS = "posts";
    private static final String DANGEROUS_POSTS = "dangerous_posts";
    private static final Filter dangerousPostFilter = new KeywordMatchFilter();

    private static final String FILTER_PATH = "Filter";
    private static final String SCRIPT = "KeywordMatchFilter";
    private static final String EXT = ".py";


    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream = builder.stream(POSTS);

        // jar내부의 resource의 파이썬 스크립트를 직접 사용할 수 없으므로 임시파일을 만들고 복사
        File tempScript = createTempScript();

        stream
                .filter((key, value) -> dangerousPostFilter.filter(tempScript, value))
                .to(DANGEROUS_POSTS);

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }

    @SneakyThrows
    private static File createTempScript() {
        InputStream is = CrawledPostAnalyzer.class.getClassLoader().getResourceAsStream(FILTER_PATH + "/" + SCRIPT + EXT);

        File tempScript = Files.createTempFile(SCRIPT, EXT).toFile();
        tempScript.deleteOnExit();

        try (OutputStream os = new FileOutputStream(tempScript)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
        }

        return tempScript;
    }
}
