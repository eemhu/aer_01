/*
 * Teragrep Azure Eventhub Reader
 * Copyright (C) 2023  Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */

package com.teragrep.aer_01;

import com.azure.identity.AzureAuthorityHosts;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.jmx.JmxReporter;
import com.teragrep.aer_01.config.*;
import com.teragrep.aer_01.config.source.EnvironmentSource;
import com.teragrep.aer_01.config.source.PropertySource;
import com.teragrep.aer_01.config.source.Sourceable;
import com.teragrep.aer_01.plugin.MappedPluginFactories;
import com.teragrep.aer_01.plugin.WrappedPluginFactoryWithConfig;
import com.teragrep.aer_01.tls.AzureSSLContextSupplier;
import com.teragrep.akv_01.plugin.PluginFactoryConfig;
import com.teragrep.akv_01.plugin.PluginMap;
import com.teragrep.rlp_01.client.IManagedRelpConnection;
import com.teragrep.rlp_01.client.ManagedRelpConnectionStub;
import com.teragrep.rlp_01.pool.Pool;
import com.teragrep.rlp_01.pool.UnboundPool;
import com.teragrep.aer_01.plugin.PluginConfiguration;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.MetricsServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

// https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-java-get-started-send?tabs=passwordless%2Croles-azure-portal

public final class Main {

    public static void main(String[] args) throws Exception {
        final MetricRegistry metricRegistry = new MetricRegistry();
        final Sourceable configSource = getConfigSource();
        final int MAX_BATCH_SIZE = new BatchConfig(configSource).maxBatchSize();
        final int prometheusPort = new MetricsConfig(configSource).prometheusPort();
        final Logger logger = Logger.getLogger(Main.class.getName());
        final String realHostname = new Hostname("localhost").hostname();

        RelpConnectionConfig relpConnectionConfig = new RelpConnectionConfig(configSource);

        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
        Slf4jReporter slf4jReporter = Slf4jReporter
                .forRegistry(metricRegistry)
                .outputTo(LoggerFactory.getLogger(EventDataConsumer.class))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        Server jettyServer = new Server(prometheusPort);

        startMetrics(jmxReporter, slf4jReporter, metricRegistry, jettyServer);

        final PluginMap pluginMap;
        try {
            pluginMap = new PluginMap(new PluginConfiguration(configSource).asJson());
        }
        catch (final IOException e) {
            throw new UncheckedIOException(e);
        }

        final Map<String, PluginFactoryConfig> pluginFactoryConfigs = pluginMap.asUnmodifiableMap();
        final String defaultPluginFactoryClassName = pluginMap.defaultPluginFactoryClassName();
        final String exceptionPluginFactoryClassName = pluginMap.exceptionPluginFactoryClassName();
        final MappedPluginFactories mappedPluginFactories = new MappedPluginFactories(
                pluginFactoryConfigs,
                defaultPluginFactoryClassName,
                exceptionPluginFactoryClassName,
                realHostname,
                new SyslogConfig(configSource),
                logger
        );

        Map<String, WrappedPluginFactoryWithConfig> pluginFactories = mappedPluginFactories.asUnmodifiableMap();
        WrappedPluginFactoryWithConfig defaultPluginFactory = mappedPluginFactories.defaultPluginFactoryWithConfig();
        WrappedPluginFactoryWithConfig exceptionPluginFactory = mappedPluginFactories.exceptionPluginFactoryWithConfig();

        Pool<IManagedRelpConnection> relpConnectionPool;
        if (configSource.source("relp.tls.mode", "none").equals("keyVault")) {
            logger.info("Using keyVault TLS mode");
            relpConnectionPool = new UnboundPool<>(
                    new ManagedRelpConnectionWithMetricsFactory(
                            logger,
                            relpConnectionConfig.asRelpConfig(),
                            "defaultOutput",
                            metricRegistry,
                            relpConnectionConfig.asSocketConfig(),
                            new AzureSSLContextSupplier()
                    ),
                    new ManagedRelpConnectionStub()
            );
        }
        else {
            logger.info("Using plain mode");
            relpConnectionPool = new UnboundPool<>(
                    new ManagedRelpConnectionWithMetricsFactory(
                            logger,
                            relpConnectionConfig.asRelpConfig(),
                            "defaultOutput",
                            metricRegistry,
                            relpConnectionConfig.asSocketConfig()
                    ),
                    new ManagedRelpConnectionStub()
            );
        }

        final DefaultOutput dOutput = new DefaultOutput(logger, relpConnectionPool);

        try (final EventDataConsumer PARTITION_PROCESSOR = new EventDataConsumer(logger, dOutput, pluginFactories, defaultPluginFactory, exceptionPluginFactory, metricRegistry)) {
            AzureConfig azureConfig = new AzureConfig(configSource);
            final ErrorContextConsumer ERROR_HANDLER = new ErrorContextConsumer();

            // create a token using the default Azure credential
            DefaultAzureCredential credential = new DefaultAzureCredentialBuilder()
                    .authorityHost(AzureAuthorityHosts.AZURE_PUBLIC_CLOUD)
                    .build();

            // Create a blob container client that you use later to build an event processor client to receive and process events
            BlobContainerAsyncClient blobContainerAsyncClient = new BlobContainerClientBuilder()
                    .credential(credential)
                    .endpoint(azureConfig.blobStorageEndpoint())
                    .containerName(azureConfig.blobStorageContainerName())
                    .buildAsyncClient();

            // Create an event processor client to receive and process events and errors.
            EventProcessorClient eventProcessorClient = new EventProcessorClientBuilder()
                    .fullyQualifiedNamespace(azureConfig.namespaceName())
                    .eventHubName(azureConfig.eventHubName())
                    .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                    .processEventBatch(PARTITION_PROCESSOR, MAX_BATCH_SIZE)
                    .processError(ERROR_HANDLER)
                    .checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient))
                    .credential(credential)
                    .buildEventProcessorClient();


            eventProcessorClient.start();

            Thread.sleep(Long.MAX_VALUE);

            eventProcessorClient.stop();

            jettyServer.stop();
            slf4jReporter.stop();
            jmxReporter.stop();
        }
    }

    private static void startMetrics(JmxReporter jmxReporter, Slf4jReporter slf4jReporter, MetricRegistry metricRegistry, Server jettyServer) {
        jmxReporter.start();
        slf4jReporter.start(1, TimeUnit.MINUTES);

        // prometheus-exporter
        CollectorRegistry.defaultRegistry.register(new DropwizardExports(metricRegistry));

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        jettyServer.setHandler(context);

        MetricsServlet metricsServlet = new MetricsServlet();
        ServletHolder servletHolder = new ServletHolder(metricsServlet);
        context.addServlet(servletHolder, "/metrics");

        // Start the webserver.
        try {
            jettyServer.start();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Sourceable getConfigSource() {
        String type = System.getProperty("config.source", "properties");

        Sourceable rv;
        if ("properties".equals(type)) {
            rv = new PropertySource();
        }
        else if ("environment".equals(type)) {
            rv = new EnvironmentSource();
        }
        else {
            throw new IllegalArgumentException("config.source not within supported types: [properties, environment]");
        }

        return rv;
    }
}
