package com.teragrep.aer_01.fakes;

import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.models.EventBatchContext;
import com.azure.messaging.eventhubs.models.LastEnqueuedEventProperties;
import com.azure.messaging.eventhubs.models.PartitionContext;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class EventBatchContextFactoryImpl implements EventBatchContextFactory {
    private final int batchSize;

    public EventBatchContextFactoryImpl(final int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public EventBatchContext eventBatchContext() {
        List<EventData> eventDataList = new ArrayList<>();
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
