package com.getindata.connectors.http.internal.status;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static java.lang.String.format;

import lombok.experimental.UtilityClass;
import org.apache.flink.util.ConfigurationException;

@UtilityClass
public class HttpCodesParser {

    private final Pattern CODE_GROUP_EXPRESSION = Pattern.compile("[1-5][xX]{2}");
    private final String DELIMITER = Pattern.quote(",");
    private final int HTTP_CODE_MIN = 100;
    private final int HTTP_CODE_MAX = 599;

    public Set<Integer> parse(String codesExpression) throws ConfigurationException {
        var whitelist = new HashSet<Integer>();
        var blacklist = new HashSet<Integer>();
        for (var rawCode : codesExpression.split(DELIMITER)) {
            var code = rawCode.trim();
            if (code.isEmpty()) {
                continue;
            }
            if (code.startsWith("!")) {
                try {
                    blacklist.add(parseHttpCode(code.substring(1)));
                    continue;
                } catch (NumberFormatException e) {
                    throw new ConfigurationException("Can not parse code " + code);
                }
            }
            try {
                whitelist.add(parseHttpCode(code));
            } catch (NumberFormatException e) {
                if (CODE_GROUP_EXPRESSION.matcher(code).matches()) {
                    var firstGroupCode = Integer.parseInt(code.substring(0, 1)) * 100;
                    var groupCodes = IntStream.range(firstGroupCode, firstGroupCode + 100)
                            .boxed().collect(Collectors.toList());
                    whitelist.addAll(groupCodes);
                } else {
                    throw new ConfigurationException("Can not parse code " + code);
                }
            }
        }

        whitelist.removeAll(blacklist);
        return Collections.unmodifiableSet(whitelist);
    }

    private Integer parseHttpCode(String str) throws ConfigurationException {
        var parsed = Integer.parseInt(str);
        if (parsed < HTTP_CODE_MIN || parsed > HTTP_CODE_MAX) {
            throw new ConfigurationException(format("Http code out of the range [%s]", parsed));
        }
        return parsed;
    }
}
