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
package com.teragrep.aer_01.fakes;

import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.models.EventBatchContext;
import com.azure.messaging.eventhubs.models.LastEnqueuedEventProperties;
import com.azure.messaging.eventhubs.models.PartitionContext;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class FakeEventBatchContextFactoryImpl implements EventBatchContextFactory {
    private final int batchSize;

    public FakeEventBatchContextFactoryImpl(final int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public EventBatchContext eventBatchContext() {
        final List<EventData> eventDataList = new ArrayList<>();
        final PartitionContext partitionContext = new PartitionContext("namespace", "eventHubName",
                "consumerGroup", "0");
        final LastEnqueuedEventProperties lastEnqueuedEventProperties = new LastEnqueuedEventProperties(1L, 100L,
                Instant.ofEpochSecond(0), Instant.ofEpochSecond(0));
        final CheckpointStore checkpointStore = new CheckpointStoreFake();

        for (int i = 0; i < batchSize; i++) {
            final EventData eventData = new EventDataFake();
            eventDataList.add(eventData);
        }

        return new EventBatchContext(partitionContext, eventDataList, checkpointStore, lastEnqueuedEventProperties);
    }
}
