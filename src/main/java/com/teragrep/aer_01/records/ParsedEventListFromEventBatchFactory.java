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
}
