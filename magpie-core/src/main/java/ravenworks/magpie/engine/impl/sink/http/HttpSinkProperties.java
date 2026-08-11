package ravenworks.magpie.engine.impl.sink.http;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import ravenworks.magpie.common.util.PropertiesUtils;
import ravenworks.magpie.engine.api.sink.DeliveryMode;

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

    /**
     * 重试落库失败时原地重试的间隔（ms）
     */
    @JsonProperty("retry.persistDelay")
    private long persistRetryDelayMs = 1000;

    @JsonProperty("retry.statusCodes")
    private String retryStatusCodesStr = "500-599,408,429";

    @JsonProperty("circuitBreaker.failureThreshold")
    private int circuitBreakerFailureThreshold = 10;

    @JsonProperty("circuitBreaker.halfOpenSuccessCount")
    private int circuitBreakerHalfOpenSuccessCount = 6;

    @JsonProperty("circuitBreaker.resetMs")
    private long circuitBreakerResetMs = 600000;

    @JsonProperty("deliveryMode")
    private String deliveryMode = "ORDERED";

    @JsonProperty("batchSize")
    private int batchSize = 100;

    @JsonProperty("commit.interval")
    private long commitInterval = 30000;

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

    public DeliveryMode resolveDeliveryMode() {
        if (this.deliveryMode == null || this.deliveryMode.isBlank()) {
            return DeliveryMode.ORDERED;
        }
        for (var v : DeliveryMode.values()) {
            if (v.name().equalsIgnoreCase(this.deliveryMode)) {
                return v;
            }
        }
        return DeliveryMode.ORDERED;
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
