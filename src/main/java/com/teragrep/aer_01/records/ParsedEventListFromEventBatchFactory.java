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
package com.teragrep.aer_01.records;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.models.EventBatchContext;
import com.azure.messaging.eventhubs.models.PartitionContext;
import com.teragrep.akv_01.event.*;
import com.teragrep.akv_01.event.metadata.offset.EventOffset;
import com.teragrep.akv_01.event.metadata.offset.EventOffsetImpl;
import com.teragrep.akv_01.event.metadata.offset.EventOffsetStub;
import com.teragrep.akv_01.event.metadata.partitionContext.EventPartitionContext;
import com.teragrep.akv_01.event.metadata.partitionContext.EventPartitionContextStub;
import com.teragrep.akv_01.event.metadata.properties.EventProperties;
import com.teragrep.akv_01.event.metadata.properties.EventPropertiesImpl;
import com.teragrep.akv_01.event.metadata.properties.EventPropertiesStub;
import com.teragrep.akv_01.event.metadata.systemProperties.EventSystemProperties;
import com.teragrep.akv_01.event.metadata.systemProperties.EventSystemPropertiesImpl;
import com.teragrep.akv_01.event.metadata.systemProperties.EventSystemPropertiesStub;
import com.teragrep.akv_01.event.metadata.time.EnqueuedTime;
import com.teragrep.akv_01.event.metadata.time.EnqueuedTimeStub;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class ParsedEventListFromEventBatchFactory {
    private final EventBatchContext eventBatchContext;
    private static final EventPartitionContext eventPartitionContextStub = new EventPartitionContextStub();
    private static final EventProperties eventPropertiesStub = new EventPropertiesStub();
    private static final EventSystemProperties eventSystemPropertiesStub = new EventSystemPropertiesStub();
    private static final EnqueuedTime enqueuedTimeStub = new EnqueuedTimeStub();
    private static final EventOffset eventOffsetStub = new EventOffsetStub();

    public ParsedEventListFromEventBatchFactory(final EventBatchContext eventBatchContext) {
        this.eventBatchContext = eventBatchContext;
    }

    public List<ParsedEvent> parsedEvents() {
        final List<ParsedEvent> rv = new ArrayList<>();
        final PartitionContext partitionContext = eventBatchContext.getPartitionContext();
        final List<EventData> eventDatas = eventBatchContext.getEvents();

        for (final EventData eventData : eventDatas) {
            final String payload = eventData.getBodyAsString();

            if (payload == null) {
                continue;
            }

            EventPartitionContext partitionCtx = eventPartitionContextStub;
            EventProperties props = eventPropertiesStub;
            EventSystemProperties systemProps = eventSystemPropertiesStub;
            EnqueuedTime enqueuedTime = enqueuedTimeStub;
            EventOffset offset = eventOffsetStub;

            if (partitionContext != null) {
                partitionCtx = new EventPartitionContextFromPojo(partitionContext);
            }

            if (eventData.getProperties() != null) {
                props = new EventPropertiesImpl(eventData.getProperties());
            }

            if (eventData.getSystemProperties() != null) {
                systemProps = new EventSystemPropertiesImpl(eventData.getSystemProperties());
            }

            if (eventData.getEnqueuedTime() != null) {
                enqueuedTime = new EnqueuedTimeFromInstant(eventData.getEnqueuedTime());
            }

            if (eventData.getOffset() != null) {
                offset = new EventOffsetImpl(eventData.getOffset().toString());
            }


            final ParsedEvent pe = new ParsedEventFactory(
                    new UnparsedEventImpl(
                            payload,
                            partitionCtx,
                            props,
                            systemProps,
                            enqueuedTime,
                            offset
                    )
            ).parsedEvent();

            rv.add(pe);
        }

        return rv;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ParsedEventListFromEventBatchFactory that = (ParsedEventListFromEventBatchFactory) o;
        return Objects.equals(eventBatchContext, that.eventBatchContext);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(eventBatchContext);
    }
}
