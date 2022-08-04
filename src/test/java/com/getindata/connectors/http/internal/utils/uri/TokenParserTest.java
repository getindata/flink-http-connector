package com.getindata.connectors.http.internal.utils.uri;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.assertj.core.api.Assertions.assertThat;

class TokenParserTest {

    @ParameterizedTest
    @CsvSource({"a,a", "aa,aa", "a a,a a", "a ,a", " a,a"})
    public void testParse(String toParse, String expected) {

        CharArrayBuffer charBuff = new CharArrayBuffer(toParse.length());
        charBuff.append(toParse);

        TokenParser tokenParser = new TokenParser();
        String actual = tokenParser.parseToken(
            charBuff,
            new ParserCursor(0, toParse.length()),
            null
        );

        assertThat(actual).isEqualTo(expected);
    }
}
