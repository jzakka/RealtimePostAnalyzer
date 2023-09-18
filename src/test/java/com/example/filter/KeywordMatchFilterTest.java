package com.example.filter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class KeywordMatchFilterTest {
    KeywordMatchFilter filter = new KeywordMatchFilter();

    @Test
    @DisplayName("파이썬 스크립트 잘 읽어오나요?")
    void readScriptTest() {
        String pythonCode = filter.extractPythonCode().trim();
        String expect = """
                import sys
                                
                data = sys.argv[1]
                                
                bad_words = ["시발", "씨발", "개새끼", "새끼", "ㅈㄴ", "염병", "옘병", "쉬불", "느금"]
                                
                for bad_word in bad_words:
                    if bad_word in data:
                        print("dangerous")
                        sys.exit()
                                
                print("safe")
                """.trim();
        Assertions.assertEquals(pythonCode, expect);
    }
}