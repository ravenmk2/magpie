package ravenworks.magpie.engine.sink.http;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import ravenworks.magpie.common.util.PropertiesUtils;
import ravenworks.magpie.engine.sink.OrderingGuarantee;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * @author Raven
 */
@Data
public class HttpSinkProperties {

    @JsonProperty(required = true)
    private String url;

    @JsonProperty("timeout")
    private int timeout = 10000;

    @JsonProperty("retry.backoff")
    private String backoff = "fixed";

    @JsonProperty("retry.delay")
    private long delayMs = 1000;

    @JsonProperty("retry.maxDelay")
    private long maxDelayMs = 30000;

    @JsonProperty("retry.inplaceAttempts")
    private int inplaceAttempts = 3;

    @JsonProperty("retry.emptyPollThreshold")
    private int emptyPollThreshold = 3;

    @JsonProperty("retry.statusCodes")
    private String retryStatusCodesStr = "500-599,408,429";

    @JsonProperty("circuitBreaker.failureThreshold")
    private int circuitBreakerFailureThreshold = 10;

    @JsonProperty("circuitBreaker.halfOpenSuccessCount")
    private int circuitBreakerHalfOpenSuccessCount = 6;

    @JsonProperty("circuitBreaker.resetMs")
    private long circuitBreakerResetMs = 600000;

    @JsonProperty("orderingGuarantee")
    private String orderingGuarantee = "ORDERED";

    @JsonProperty("batchSize")
    private int batchSize = 100;

    private Set<Integer> retryStatusCodes;

    public static HttpSinkProperties of(Map<String, Object> props) {
        var config = new HttpSinkProperties();
        PropertiesUtils.bind(config, props);
        if (config.url == null || config.url.isBlank()) {
            throw new IllegalArgumentException("HttpSinkConnector requires 'url' property");
        }
        config.retryStatusCodes = parseStatusCodes(config.retryStatusCodesStr);
        return config;
    }

    public OrderingGuarantee resolveOrderingGuarantee() {
        if (this.orderingGuarantee == null || this.orderingGuarantee.isBlank()) {
            return OrderingGuarantee.ORDERED;
        }
        for (var v : OrderingGuarantee.values()) {
            if (v.name().equalsIgnoreCase(this.orderingGuarantee)) {
                return v;
            }
        }
        return OrderingGuarantee.ORDERED;
    }

    static Set<Integer> parseStatusCodes(String str) {
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
