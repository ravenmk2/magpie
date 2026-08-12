package ravenworks.magpie.soak.loadgen;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * 发布器：向 magpie 发布端点 POST structured CloudEvent，多实例按序故障转移。
 * 503（no_subscriber，该实例未运行 source 连接器）与 5xx/IO 错误换下一实例；
 * 其余 4xx（400 非法消息、403 topic 不允许）是配置错误，换实例无意义，记 rejected 后返回失败。
 * 计数器按 topic 启动即注册（恒有 0 值序列），看板可区分"零错误"与"没采集到数据"。
 */
@Slf4j
public class Publisher {

    private final List<String> publishUrls;
    private final HttpClient client;
    private final Duration timeout;
    private final Map<String, Counters> topicCounters;
    private volatile int lastGood;

    public Publisher(List<String> endpoints, String source, Duration timeout,
                     List<String> topics, MeterRegistry metrics) {
        this.publishUrls = endpoints.stream()
                .map(e -> stripTrailingSlash(e) + "/api/v1/publish/" + source)
                .toList();
        this.timeout = timeout;
        this.client = HttpClient.newBuilder()
                .connectTimeout(timeout)
                .build();
        this.topicCounters = topics.stream().collect(Collectors.toMap(Function.identity(),
                topic -> new Counters(
                        metrics.counter("soak.sent", "topic", topic),
                        metrics.counter("soak.send.errors", "topic", topic),
                        metrics.counter("soak.send.rejected", "topic", topic))));
    }

    /**
     * @return true 表示某实例确认收到；false 表示全部实例失败或不可重试错误（调用方重试同一消息）
     */
    public boolean send(String topic, byte[] body) {
        Counters counters = this.topicCounters.get(topic);
        for (int i = 0; i < this.publishUrls.size(); i++) {
            int idx = (this.lastGood + i) % this.publishUrls.size();
            try {
                var request = HttpRequest.newBuilder()
                        .uri(URI.create(this.publishUrls.get(idx)))
                        .timeout(this.timeout)
                        .header("Content-Type", "application/cloudevents+json")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                        .build();
                var response = this.client.send(request, HttpResponse.BodyHandlers.discarding());
                int status = response.statusCode();
                if (status >= 200 && status < 300) {
                    this.lastGood = idx;
                    counters.sent.increment();
                    return true;
                }
                if (status == 503 || status >= 500) {
                    counters.errors.increment();
                    log.warn("publish to {} failed with HTTP {}, trying next endpoint", this.publishUrls.get(idx), status);
                    continue;
                }
                counters.rejected.increment();
                log.error("publish rejected with HTTP {}, check soak seed config", status);
                return false;
            } catch (IOException e) {
                counters.errors.increment();
                log.warn("publish to {} failed: {}, trying next endpoint", this.publishUrls.get(idx), e.toString());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    private static String stripTrailingSlash(String endpoint) {
        return endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
    }

    private record Counters(Counter sent, Counter errors, Counter rejected) {

    }

}
