package com.example.filter;

import com.example.CrawledPostAnalyzer;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class KeywordMatchFilter implements Filter{
    private static Logger logger = LoggerFactory.getLogger(KeywordMatchFilter.class);
    private static final String FILTER_PATH = "Filter";
    private static final String FILTER_NAME = "KeywordMatchFilter.py";

    @SneakyThrows
    public String extractPythonCode(){
        InputStream pythonCodesStream = getPythonScriptStream();

        return new String(pythonCodesStream.readAllBytes(), StandardCharsets.UTF_8);
    }

    private InputStream getPythonScriptStream() {
        return CrawledPostAnalyzer.class.getClassLoader()
                .getResourceAsStream(FILTER_PATH + "/" + FILTER_NAME);
    }


    @SneakyThrows
    @Override
    public boolean filter(File script, String message) {
//        String filterPath = getFilterPath();
        ProcessBuilder judgeBuilder = new ProcessBuilder("python", script.getAbsolutePath(), message);

        Process judge = judgeBuilder.start();
        int exitCode = judge.waitFor();

        if (exitCode != 0) {
            throw new RuntimeException("게시글 분류기가 비정상 종료돼었음 종료코드=" + exitCode);
        }

        String result = getResult(judge);

        return "dangerous".equals(result);
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
