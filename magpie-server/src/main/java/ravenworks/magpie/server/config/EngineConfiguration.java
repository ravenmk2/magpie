package ravenworks.magpie.server.config;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ravenworks.magpie.domain.repository.*;
import ravenworks.magpie.engine.lock.LeaderLock;
import ravenworks.magpie.engine.lock.LeaderLockImpl;
import ravenworks.magpie.engine.retry.RetryMessageStore;
import ravenworks.magpie.engine.retry.RetryMessageStoreImpl;
import ravenworks.magpie.engine.runtime.Coordinator;
import ravenworks.magpie.engine.sink.*;
import ravenworks.magpie.engine.sink.http.HttpSinkProvider;
import ravenworks.magpie.engine.sink.print.PrintSinkProvider;
import ravenworks.magpie.engine.source.*;
import ravenworks.magpie.engine.source.http.HttpSourceProvider;
import ravenworks.magpie.engine.source.http.HttpSourceRouter;
import ravenworks.magpie.engine.source.http.HttpSourceRouterImpl;
import ravenworks.magpie.engine.source.mysql.MySqlPollSourceProvider;
import ravenworks.magpie.engine.source.sample.SampleSourceProvider;
import ravenworks.magpie.engine.stream.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


@Slf4j
@Configuration
public class EngineConfiguration {

    @Bean
    public static LeaderLock leaderLock(@NonNull LeaderLockRepository lockRepository) {
        return new LeaderLockImpl(lockRepository);
    }

    @Bean
    public static StreamRegistry streamRegistry(@NonNull TopicRepository topicRepository) {
        return new StreamRegistryImpl(topicRepository);
    }

    @Bean
    public static SourceRegistry sourceRegistry(@NonNull SourceRepository sourceRepository) {
        return new SourceRegistryImpl(sourceRepository);
    }

    @Bean
    public static HttpSourceRouter httpSourceRouter() {
        return new HttpSourceRouterImpl();
    }

    @Bean
    public static SourceFactory sourceFactory(@NonNull List<SourceProvider> providers,
                                              @NonNull HttpSourceRouter httpSourceRouter) {
        var merged = new ArrayList<>(providers);
        merged.add(new SampleSourceProvider());
        merged.add(new MySqlPollSourceProvider());
        merged.add(new HttpSourceProvider(httpSourceRouter));
        return new SourceFactoryImpl(merged);
    }

    @Bean
    public static TargetRegistry targetRegistry(@NonNull TargetRepository targetRepository) {
        return new TargetRegistryImpl(targetRepository);
    }

    @Bean
    public static OffsetTracker offsetTracker(@NonNull ConsumerOffsetRepository consumerOffsetRepository) {
        return new OffsetTrackerImpl(consumerOffsetRepository);
    }

    @Bean
    public static SinkFactory sinkFactory(@NonNull List<SinkProvider> providers,
                                          @NonNull StreamRegistry streamRegistry,
                                          @NonNull RetryMessageStore retryMessageStore) {
        var merged = new ArrayList<>(providers);
        merged.add(new PrintSinkProvider(streamRegistry));
        merged.add(new HttpSinkProvider(streamRegistry, retryMessageStore));
        return new SinkFactoryImpl(merged);
    }

    @Bean
    public static RetryMessageStore retryMessageStore(@NonNull MessageLogRepository messageLogRepository,
                                                      @NonNull RetryMessageRepository retryMessageRepository) {
        return new RetryMessageStoreImpl(messageLogRepository, retryMessageRepository);
    }

    @Bean
    public static Coordinator coordinator(@NonNull LeaderLock leaderLock,
                                          @NonNull StreamRegistry streamRegistry,
                                          @NonNull StreamProvider streamProvider,
                                          @NonNull SourceRegistry sourceRegistry,
                                          @NonNull SourceFactory sourceFactory,
                                          @NonNull TargetRegistry targetRegistry,
                                          @NonNull SinkFactory sinkFactory,
                                          @NonNull RoutingStreamProducer streamProducer) {
        return new Coordinator(leaderLock, streamRegistry, streamProvider,
                sourceRegistry, sourceFactory, targetRegistry, sinkFactory, streamProducer);
    }

    @Bean
    public static RoutingStreamProducer routingStreamProducer(@NonNull StreamProvider streamProvider,
                                                              @NonNull StreamRegistry streamRegistry) {
        return new RoutingStreamProducer(streamProvider, streamRegistry);
    }

    @Bean
    public static SmartLifecycle coordinatorLifecycle(@NonNull Coordinator coordinator) {
        return new SmartLifecycle() {

            @Override
            public void start() {
                coordinator.start();
            }

            @Override
            public void stop() {
                try {
                    coordinator.shutdown().get(90, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Coordinator shutdown interrupted", e);
                } catch (Exception e) {
                    log.error("Coordinator shutdown failed or timed out", e);
                }
            }

            @Override
            public boolean isRunning() {
                // 状态直接派生自 Coordinator 的事件循环（SHUTTING_DOWN 视为运行中），
                // 避免自持标志与真实状态脱节；Spring 仅在 true 时回调 stop()
                return coordinator.isRunning();
            }
        };
    }

}
