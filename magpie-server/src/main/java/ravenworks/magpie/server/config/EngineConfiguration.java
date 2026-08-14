package ravenworks.magpie.server.config;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ravenworks.magpie.domain.repository.*;
import ravenworks.magpie.engine.api.election.LeaderElection;
import ravenworks.magpie.engine.api.retry.RetryMessageStore;
import ravenworks.magpie.engine.api.sink.SinkFactory;
import ravenworks.magpie.engine.api.sink.SinkProvider;
import ravenworks.magpie.engine.api.sink.TargetRegistry;
import ravenworks.magpie.engine.api.source.SourceFactory;
import ravenworks.magpie.engine.api.source.SourceProvider;
import ravenworks.magpie.engine.api.source.SourceRegistry;
import ravenworks.magpie.engine.api.source.http.HttpSourceRouter;
import ravenworks.magpie.engine.api.stream.OffsetTracker;
import ravenworks.magpie.engine.api.stream.StreamProvider;
import ravenworks.magpie.engine.api.stream.StreamRegistry;
import ravenworks.magpie.engine.impl.election.LeaderElectionImpl;
import ravenworks.magpie.engine.impl.retry.RetryMessageStoreImpl;
import ravenworks.magpie.engine.impl.runtime.Coordinator;
import ravenworks.magpie.engine.impl.sink.SinkFactoryImpl;
import ravenworks.magpie.engine.impl.sink.TargetRegistryImpl;
import ravenworks.magpie.engine.impl.sink.http.HttpSinkProvider;
import ravenworks.magpie.engine.impl.sink.print.PrintSinkProvider;
import ravenworks.magpie.engine.impl.source.SourceFactoryImpl;
import ravenworks.magpie.engine.impl.source.SourceRegistryImpl;
import ravenworks.magpie.engine.impl.source.http.HttpSourceProvider;
import ravenworks.magpie.engine.impl.source.http.HttpSourceRouterImpl;
import ravenworks.magpie.engine.impl.source.mysql.MySqlPollSourceProvider;
import ravenworks.magpie.engine.impl.source.sample.SampleSourceProvider;
import ravenworks.magpie.engine.impl.stream.OffsetTrackerImpl;
import ravenworks.magpie.engine.impl.stream.StreamRegistryImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


@Slf4j
@Configuration
public class EngineConfiguration {

    @Bean
    public static LeaderElection leaderElection(@NonNull LeaderLockRepository lockRepository) {
        return new LeaderElectionImpl(lockRepository);
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
                                              @NonNull HttpSourceRouter httpSourceRouter,
                                              @NonNull StreamRegistry streamRegistry) {
        var merged = new ArrayList<>(providers);
        merged.add(new SampleSourceProvider(streamRegistry));
        merged.add(new MySqlPollSourceProvider(streamRegistry));
        merged.add(new HttpSourceProvider(httpSourceRouter, streamRegistry));
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
    public static Coordinator coordinator(@NonNull LeaderElection leaderElection,
                                          @NonNull StreamRegistry streamRegistry,
                                          @NonNull StreamProvider streamProvider,
                                          @NonNull SourceRegistry sourceRegistry,
                                          @NonNull SourceFactory sourceFactory,
                                          @NonNull TargetRegistry targetRegistry,
                                          @NonNull SinkFactory sinkFactory) {
        return new Coordinator(leaderElection, streamRegistry, streamProvider,
                sourceRegistry, sourceFactory, targetRegistry, sinkFactory);
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
