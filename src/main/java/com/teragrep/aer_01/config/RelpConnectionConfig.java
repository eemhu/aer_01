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
package com.teragrep.aer_01.config;

import com.teragrep.aer_01.config.source.Sourceable;
import com.teragrep.rlp_01.client.RelpConfig;
import com.teragrep.rlp_01.client.SocketConfig;
import com.teragrep.rlp_01.client.SocketConfigImpl;

import java.time.Duration;

/**
 * Configuration object for RELP connection related options
 */
public final class RelpConnectionConfig {

    private final int connectTimeout;
    private final int readTimeout;
    private final int writeTimeout;
    private final int reconnectInterval;
    private final int port;
    private final String address;
    private final int rebindRequestAmount;
    private final boolean rebindEnabled;
    private final Duration maxIdle;
    private final boolean maxIdleEnabled;
    private final boolean keepAlive;

    /**
     * Initialize RelpConnectionConfig using a {@link Sourceable} as the source.
     * @param configSource {@link Sourceable}
     */
    public RelpConnectionConfig(final Sourceable configSource) {
        this(
                Integer.parseInt(configSource.source("relp.connection.timeout", "2500")),
                Integer.parseInt(configSource.source("relp.transaction.read.timeout", "1500")),
                Integer.parseInt(configSource.source("relp.transaction.write.timeout", "1500")),
                Integer.parseInt(configSource.source("relp.connection.retry.interval", "500")),
                Integer.parseInt(configSource.source("relp.connection.port", "601")),
                configSource.source("relp.connection.address", "localhost"),
                Integer.parseInt(configSource.source("relp.rebind.request.amount", "100000")),
                Boolean.parseBoolean(configSource.source("relp.rebind.enabled", "true")),
                Duration.parse(configSource.source("relp.max.idle.duration", Duration.ofMillis(150000L).toString())),
                Boolean.parseBoolean(configSource.source("relp.max.idle.enabled", "false")),
                Boolean.parseBoolean(configSource.source("relp.connection.keepalive", "true"))
        );
    }

    /**
     * Initialize RelpConnectionConfig directly with the configuration values.
     * @param connectTimeout relp.connection.timeout
     * @param readTimeout relp.transaction.read.timeout
     * @param writeTimeout relp.transaction.write.timeout
     * @param reconnectInt relp.connection.retry.interval
     * @param port relp.connection.port
     * @param addr relp.connection.address
     * @param rebindRequestAmount relp.rebind.request.amount
     * @param rebindEnabled relp.rebind.enabled
     * @param maxIdle relp.max.idle.duration
     * @param maxIdleEnabled relp.max.idle.enabled
     * @param keepAlive relp.connection.keepalive
     */
    public RelpConnectionConfig(
            final int connectTimeout,
            final int readTimeout,
            final int writeTimeout,
            final int reconnectInt,
            final int port,
            final String addr,
            final int rebindRequestAmount,
            final boolean rebindEnabled,
            final Duration maxIdle,
            final boolean maxIdleEnabled,
            final boolean keepAlive
    ) {
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.writeTimeout = writeTimeout;
        this.reconnectInterval = reconnectInt;
        this.port = port;
        this.address = addr;
        this.rebindRequestAmount = rebindRequestAmount;
        this.rebindEnabled = rebindEnabled;
        this.maxIdle = maxIdle;
        this.maxIdleEnabled = maxIdleEnabled;
        this.keepAlive = keepAlive;
    }

    /**
     * @return relp.connection.timeout
     */
    public int connectTimeout() {
        return connectTimeout;
    }

    /**
     * @return relp.transaction.read.timeout
     */
    public int readTimeout() {
        return readTimeout;
    }

    /**
     * @return relp.transaction.write.timeout
     */
    public int writeTimeout() {
        return writeTimeout;
    }

    /**
     * @return relp.connection.retry.interval
     */
    public int reconnectInterval() {
        return reconnectInterval;
    }

    /**
     * @return relp.connection.port
     */
    public int relpPort() {
        return port;
    }

    /**
     * @return relp.connection.address
     */
    public String relpAddress() {
        return address;
    }

    /**
     * @return relp.max.idle.enabled
     */
    public boolean maxIdleEnabled() {
        return maxIdleEnabled;
    }

    /**
     * @return relp.max.idle.duration
     */
    public Duration maxIdle() {
        return maxIdle;
    }

    /**
     * @return relp.rebind.enabled
     */
    public boolean rebindEnabled() {
        return rebindEnabled;
    }

    /**
     * @return relp.rebind.request.amount
     */
    public int rebindRequestAmount() {
        return rebindRequestAmount;
    }

    /**
     * @return relp.connection.keepalive
     */
    public boolean keepAlive() {
        return keepAlive;
    }

    /**
     * @return rlp_01 RelpConfig object
     */
    public RelpConfig asRelpConfig() {
        return new RelpConfig(
                relpAddress(),
                relpPort(),
                reconnectInterval(),
                rebindRequestAmount(),
                rebindEnabled(),
                maxIdle(),
                maxIdleEnabled()
        );
    }

    /**
     * @return rlp_01 SocketConfig object
     */
    public SocketConfig asSocketConfig() {
        return new SocketConfigImpl(readTimeout(), writeTimeout(), connectTimeout(), keepAlive());
    }
}
