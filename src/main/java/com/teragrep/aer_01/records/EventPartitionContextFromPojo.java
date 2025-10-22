package com.teragrep.aer_01.records;

import com.azure.messaging.eventhubs.models.PartitionContext;
import com.teragrep.akv_01.event.metadata.partitionContext.EventPartitionContext;

import java.util.HashMap;
import java.util.Map;

public final class EventPartitionContextFromPojo implements EventPartitionContext {
    private final PartitionContext partitionContext;
    public EventPartitionContextFromPojo(final PartitionContext partitionContext) {
        this.partitionContext = partitionContext;
    }
    @Override
    public Map<String, Object> asMap() {
        final Map<String, Object> m = new HashMap<>();
        m.put("FullyQualifiedNamespace", partitionContext.getFullyQualifiedNamespace());
        m.put("EventHubName",  partitionContext.getEventHubName());
        m.put("PartitionId", partitionContext.getPartitionId());
        m.put("ConsumerGroup", partitionContext.getConsumerGroup());
        return m;
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
