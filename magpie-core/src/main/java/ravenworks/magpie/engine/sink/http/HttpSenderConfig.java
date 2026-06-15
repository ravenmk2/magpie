package ravenworks.magpie.engine.sink.http;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import ravenworks.magpie.common.util.PropertiesUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;


@Data
public class HttpSenderConfig {

    @JsonProperty(required = true)
    String url;

    @JsonProperty("timeout")
    int timeout = 10000;

    @JsonProperty("retry.backoff")
    String backoff = "fixed";

    @JsonProperty("retry.delay")
    long delayMs = 1000;

    @JsonProperty("retry.maxDelay")
    long maxDelayMs = 30000;

    @JsonProperty("retry.inplaceAttempts")
    int maxAttempts = 3;

    @JsonProperty("retry.statusCodes")
    String retryStatusCodesStr = "500-599,408,429";

    Set<Integer> retryStatusCodes;

    public static HttpSenderConfig of(Map<String, Object> props) {
        var config = new HttpSenderConfig();
        PropertiesUtils.bind(config, props);
        if (config.url == null || config.url.isBlank()) {
            throw new IllegalArgumentException("url is required");
        }
        config.retryStatusCodes = parseStatusCodes(config.retryStatusCodesStr);
        return config;
    }

    private static Set<Integer> parseStatusCodes(String str) {
        Set<Integer> codes = new HashSet<>();
        for (String part : str.split(",")) {
            part = part.trim();
            if (part.contains("-")) {
                String[] range = part.split("-", 2);
                int start = Integer.parseInt(range[0].trim());
                int end = Integer.parseInt(range[1].trim());
                for (int c = start; c <= end; c++) {
                    codes.add(c);
                }
            } else if (!part.isEmpty()) {
                codes.add(Integer.parseInt(part));
            }
        }
        return codes;
    }

}
