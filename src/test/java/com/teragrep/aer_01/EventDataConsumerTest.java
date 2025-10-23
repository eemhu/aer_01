package com.teragrep.aer_01;

import com.codahale.metrics.MetricRegistry;
import com.teragrep.aer_01.config.RelpConnectionConfig;
import com.teragrep.aer_01.fakes.EventBatchContextFactoryImpl;
import com.teragrep.aer_01.plugin.WrappedPluginFactoryWithConfig;
import com.teragrep.akv_01.plugin.PluginFactoryConfigImpl;
import com.teragrep.akv_01.plugin.PluginFactoryInitialization;
import com.teragrep.net_01.channel.socket.PlainFactory;
import com.teragrep.net_01.eventloop.EventLoop;
import com.teragrep.net_01.eventloop.EventLoopFactory;
import com.teragrep.net_01.server.Server;
import com.teragrep.net_01.server.ServerFactory;
import com.teragrep.rlo_06.RFC5424Frame;
import com.teragrep.rlp_01.client.ManagedRelpConnectionStub;
import com.teragrep.rlp_01.pool.UnboundPool;
import com.teragrep.rlp_03.frame.FrameDelegationClockFactory;
import com.teragrep.rlp_03.frame.delegate.DefaultFrameDelegate;
import com.teragrep.rlp_03.frame.delegate.FrameContext;
import com.teragrep.rlp_03.frame.delegate.FrameDelegate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public final class EventDataConsumerTest {
    private Server server;
    private EventLoop eventLoop;
    private Thread eventLoopThread;
    private ExecutorService executorService;
    private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

    @BeforeEach
    void setup() {
        messages.clear();
        this.executorService = Executors.newFixedThreadPool(1);
        Consumer<FrameContext> syslogConsumer = new Consumer<FrameContext>() {
            @Override
            public synchronized void accept(FrameContext frameContext) {
                final String payload = frameContext.relpFrame().payload().toString();
                messages.add(payload);
            }
        };

        Supplier<FrameDelegate> frameDelegateSupplier = () -> new DefaultFrameDelegate(syslogConsumer);

        EventLoopFactory eventLoopFactory = new EventLoopFactory();
        this.eventLoop = Assertions.assertDoesNotThrow(eventLoopFactory::create);

        this.eventLoopThread = new Thread(eventLoop);
        eventLoopThread.start();

        ServerFactory serverFactory = new ServerFactory(
                eventLoop,
                executorService,
                new PlainFactory(),
                new FrameDelegationClockFactory(frameDelegateSupplier)
        );
        this.server = Assertions.assertDoesNotThrow(() -> serverFactory.create(1601));
    }

    @AfterEach
    void teardown() {
        eventLoop.stop();
        Assertions.assertDoesNotThrow(() -> eventLoopThread.join());
        executorService.shutdown();
        Assertions.assertDoesNotThrow(() -> server.close());
    }

    @Test
    void testSendEventBatch() {
        MetricRegistry metricRegistry = new MetricRegistry();
        RelpConnectionConfig relpConfig = new RelpConnectionConfig(
                2500, 1500, 1500, 500, 1601, "localhost", 100_000, true, Duration.ofMillis(150_000L),
                false, true
        );
        DefaultOutput output = new DefaultOutput(
                Logger.getAnonymousLogger(),
                new UnboundPool<>(
                        new ManagedRelpConnectionWithMetricsFactory(
                                Logger.getAnonymousLogger(),
                                "defaultOutput",
                                metricRegistry,
                                relpConfig.asRelpConfig()
                        ),
                        new ManagedRelpConnectionStub()
                )
        );
        EventDataConsumer edc = new EventDataConsumer(
                Logger.getAnonymousLogger(),
                output,
                new HashMap<>(),
                Assertions.assertDoesNotThrow(() -> new WrappedPluginFactoryWithConfig(new PluginFactoryInitialization("com.teragrep.aer_01.plugin.DefaultPluginFactory").pluginFactory(), new PluginFactoryConfigImpl("com.teragrep.aer_01.plugin.DefaultPluginFactory","{\"realHostname\":\"localhost\",\"syslogHostname\":\"localhost\",\"syslogAppname\":\"aer-01\"}"))),
                Assertions.assertDoesNotThrow(() -> new WrappedPluginFactoryWithConfig(new PluginFactoryInitialization("com.teragrep.aer_01.plugin.DefaultPluginFactory").pluginFactory(), new PluginFactoryConfigImpl("com.teragrep.aer_01.plugin.DefaultPluginFactory","{\"realHostname\":\"localhost\",\"syslogHostname\":\"localhost\",\"syslogAppname\":\"aer-01\"}"))),
                metricRegistry
        );

        edc.accept(new EventBatchContextFactoryImpl(10).eventBatchContext());
        Assertions.assertEquals(10, messages.size());

        int loops = 0;
        for (final String message : messages) {
            final RFC5424Frame frame = new RFC5424Frame(false);
            frame.load(new ByteArrayInputStream(message.getBytes(StandardCharsets.UTF_8)));
            Assertions.assertTrue(Assertions.assertDoesNotThrow(frame::next));
            Assertions.assertEquals("foo", frame.msg.toString());
            Assertions.assertEquals("localhost", frame.hostname.toString());
            Assertions.assertEquals("aer-01", frame.appName.toString());
            Assertions.assertEquals("0", frame.msgId.toString());

            final Map<String, Map<String, String>> sdElems = frame.structuredData.sdElements
                    .stream()
                    .collect(
                            Collectors
                                    .toMap(
                                            (sde -> sde.sdElementId.toString()),
                                            (sde -> sde.sdParams
                                                    .stream()
                                                    .collect(
                                                            Collectors
                                                                    .toMap(
                                                                            sdp -> sdp.sdParamKey.toString(),
                                                                            sdp -> sdp.sdParamValue.toString()
                                                                    )
                                                    ))
                                    )
                    );
            Assertions.assertEquals("{}", sdElems.get("aer_01_event@48577").get("properties"));

            loops++;
        }

        Assertions.assertEquals(10, loops);

    }

    @Test
    void testMetrics() {
        MetricRegistry metricRegistry = new MetricRegistry();
        RelpConnectionConfig relpConfig = new RelpConnectionConfig(
                2500, 1500, 1500, 500, 1601, "localhost", 100_000, true, Duration.ofMillis(150_000L),
                false, true
        );
        DefaultOutput output = new DefaultOutput(
                Logger.getAnonymousLogger(),
                new UnboundPool<>(
                        new ManagedRelpConnectionWithMetricsFactory(
                                Logger.getAnonymousLogger(),
                                "defaultOutput",
                                metricRegistry,
                                relpConfig.asRelpConfig()
                        ),
                        new ManagedRelpConnectionStub()
                )
        );
        EventDataConsumer edc = new EventDataConsumer(
                Logger.getAnonymousLogger(),
                output,
                new HashMap<>(),
                Assertions.assertDoesNotThrow(() -> new WrappedPluginFactoryWithConfig(new PluginFactoryInitialization("com.teragrep.aer_01.plugin.DefaultPluginFactory").pluginFactory(), new PluginFactoryConfigImpl("com.teragrep.aer_01.plugin.DefaultPluginFactory","{\"realHostname\":\"localhost\",\"syslogHostname\":\"localhost\",\"syslogAppname\":\"aer-01\"}"))),
                Assertions.assertDoesNotThrow(() -> new WrappedPluginFactoryWithConfig(new PluginFactoryInitialization("com.teragrep.aer_01.plugin.DefaultPluginFactory").pluginFactory(), new PluginFactoryConfigImpl("com.teragrep.aer_01.plugin.DefaultPluginFactory","{\"realHostname\":\"localhost\",\"syslogHostname\":\"localhost\",\"syslogAppname\":\"aer-01\"}"))),
                metricRegistry
        );

        edc.accept(new EventBatchContextFactoryImpl(10).eventBatchContext());
        Assertions.assertEquals(10, messages.size());
        Assertions.assertEquals(10L, metricRegistry.counter("com.teragrep.aer_01.DefaultOutput.<[defaultOutput]>.records").getCount());
        Assertions.assertEquals(1L, metricRegistry.counter("com.teragrep.aer_01.DefaultOutput.<[defaultOutput]>.connects").getCount());
        Assertions.assertEquals(0L, metricRegistry.counter("com.teragrep.aer_01.DefaultOutput.<[defaultOutput]>.retriedConnects").getCount());
        Assertions.assertEquals(0L, metricRegistry.counter("com.teragrep.aer_01.DefaultOutput.<[defaultOutput]>.resends").getCount());
        Assertions.assertEquals(4750L, metricRegistry.counter("com.teragrep.aer_01.DefaultOutput.<[defaultOutput]>.bytes").getCount());

    }
}
