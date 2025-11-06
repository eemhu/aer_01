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
package com.teragrep.aer_01.hostname;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Objects;

public final class Hostname {

    private final HostnameSource hostnameSource;
    private final String defaultHostname;
    private static final Logger LOGGER = LoggerFactory.getLogger(Hostname.class);

    public Hostname(final String defaultHostname) {
        this(defaultHostname, new JavaHostnameSource());
    }

    public Hostname(final String defaultHostname, final HostnameSource hostnameSource) {
        this.hostnameSource = hostnameSource;
        this.defaultHostname = defaultHostname;
    }

    public String hostname() {
        String hostname;
        try {
            hostname = hostnameSource.hostname();
        }
        catch (final UnknownHostException e) {
            hostname = defaultHostname;
            LOGGER.warn("Could not determine hostname, defaulting to <{}>", defaultHostname, e);
        }
        return hostname;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Hostname hostname = (Hostname) o;
        return Objects.equals(hostnameSource, hostname.hostnameSource)
                && Objects.equals(defaultHostname, hostname.defaultHostname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostnameSource, defaultHostname);
    }
}
