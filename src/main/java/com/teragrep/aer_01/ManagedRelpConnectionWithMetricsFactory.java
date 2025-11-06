/*
 * Teragrep syslog bridge for Microsoft Azure EventHub
 * Copyright (C) 2023-2025 Suomen Kanuuna Oy
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

import com.codahale.metrics.MetricRegistry;
import com.teragrep.rlp_01.RelpConnection;
import com.teragrep.rlp_01.client.*;

import java.util.function.Supplier;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public final class ManagedRelpConnectionWithMetricsFactory implements Supplier<IManagedRelpConnection> {

    private final RelpConfig relpConfig;
    private final SocketConfig socketConfig;
    private final SSLContextSupplier sslContextSupplier;
    private final String name;
    private final MetricRegistry metricRegistry;
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedRelpConnectionWithMetricsFactory.class);

    public ManagedRelpConnectionWithMetricsFactory(
            final String name,
            final MetricRegistry metricRegistry,
            final RelpConfig relpConfig
    ) {
        this(relpConfig, name, metricRegistry, new SocketConfigDefault());
    }

    public ManagedRelpConnectionWithMetricsFactory(
            final RelpConfig relpConfig,
            final String name,
            final MetricRegistry metricRegistry,
            final SocketConfig socketConfig
    ) {
        this(relpConfig, name, metricRegistry, socketConfig, new SSLContextSupplierStub());
    }

    public ManagedRelpConnectionWithMetricsFactory(
            final RelpConfig relpConfig,
            final String name,
            final MetricRegistry metricRegistry,
            final SSLContextSupplier sslContextSupplier
    ) {
        this(relpConfig, name, metricRegistry, new SocketConfigDefault(), sslContextSupplier);
    }

    public ManagedRelpConnectionWithMetricsFactory(
            final RelpConfig relpConfig,
            final String name,
            final MetricRegistry metricRegistry,
            final SocketConfig socketConfig,
            final SSLContextSupplier sslContextSupplier
    ) {
        this.relpConfig = relpConfig;
        this.name = name;
        this.metricRegistry = metricRegistry;
        this.socketConfig = socketConfig;
        this.sslContextSupplier = sslContextSupplier;
    }

    @Override
    public IManagedRelpConnection get() {
        LOGGER.info("get() called for new IManagedRelpConnection");
        final IRelpConnection relpConnection;
        if (sslContextSupplier.isStub()) {
            relpConnection = new RelpConnectionWithConfig(new RelpConnection(), relpConfig);
        }
        else {
            relpConnection = new RelpConnectionWithConfig(
                    new RelpConnection(() -> sslContextSupplier.get().createSSLEngine()),
                    relpConfig
            );
        }

        relpConnection.setReadTimeout(socketConfig.readTimeout());
        relpConnection.setWriteTimeout(socketConfig.writeTimeout());
        relpConnection.setConnectionTimeout(socketConfig.connectTimeout());
        relpConnection.setKeepAlive(socketConfig.keepAlive());

        final IManagedRelpConnection managedRelpConnection = new ManagedRelpConnectionWithMetrics(
                relpConnection,
                name,
                metricRegistry
        );

        final IManagedRelpConnection connectionWithPossibleRebindEnabled;
        if (relpConfig.rebindEnabled) {
            connectionWithPossibleRebindEnabled = new RebindableRelpConnection(
                    managedRelpConnection,
                    relpConfig.rebindRequestAmount
            );
        }
        else {
            connectionWithPossibleRebindEnabled = managedRelpConnection;
        }

        final IManagedRelpConnection connectionWithPossibleMaxIdleEnabled;
        if (relpConfig.maxIdleEnabled) {
            connectionWithPossibleMaxIdleEnabled = new RenewableRelpConnection(
                    connectionWithPossibleRebindEnabled,
                    relpConfig.maxIdle
            );
        }
        else {
            connectionWithPossibleMaxIdleEnabled = connectionWithPossibleRebindEnabled;
        }

        LOGGER.info("returning new managedRelpConnection");
        return connectionWithPossibleMaxIdleEnabled;
    }
}
