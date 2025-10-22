package com.teragrep.aer_01.records;

import com.azure.messaging.eventhubs.models.EventContext;
import com.teragrep.akv_01.event.ParsedEvent;
import com.teragrep.akv_01.event.ParsedEventFactory;
import com.teragrep.akv_01.event.UnparsedEvent;
import com.teragrep.akv_01.event.UnparsedEventImpl;
import com.teragrep.akv_01.event.metadata.offset.EventOffset;
import com.teragrep.akv_01.event.metadata.offset.EventOffsetImpl;
import com.teragrep.akv_01.event.metadata.partitionContext.EventPartitionContext;
import com.teragrep.akv_01.event.metadata.properties.EventProperties;
import com.teragrep.akv_01.event.metadata.properties.EventPropertiesImpl;
import com.teragrep.akv_01.event.metadata.systemProperties.EventSystemProperties;
import com.teragrep.akv_01.event.metadata.systemProperties.EventSystemPropertiesImpl;
import com.teragrep.akv_01.event.metadata.time.EnqueuedTime;

public final class EventContextAsParsedEvent {
    private final EventContext eventContext;
    public EventContextAsParsedEvent(final EventContext eventContext) {
        this.eventContext = eventContext;
    }

    public ParsedEvent parsedEvent() {
        ParsedEvent rv = null; //FIXME: Remove null assignment - Introduce stubbable ParsedEvent?
        if (eventContext != null && eventContext.getEventData() != null
        && eventContext.getEventData().getBodyAsString() != null) {

            final String payload = eventContext.getEventData().getBodyAsString();
            final EventPartitionContext partitionCtx = new EventPartitionContextFromPojo(eventContext.getPartitionContext());
            final EventProperties props = new EventPropertiesImpl(eventContext.getEventData().getProperties());
            final EventSystemProperties sysProps = new EventSystemPropertiesImpl(eventContext.getEventData().getSystemProperties());
            final EnqueuedTime enqueuedTime = new EnqueuedTimeFromInstant(eventContext.getEventData().getEnqueuedTime());
            final EventOffset offset = new EventOffsetImpl(String.valueOf(eventContext.getEventData().getOffset()));
            UnparsedEvent unparsedEvent = new UnparsedEventImpl(
                    payload,
                    partitionCtx,
                    props,
                    sysProps,
                    enqueuedTime,
                    offset
            );
            rv = new ParsedEventFactory(unparsedEvent).parsedEvent();
        }
        return rv;
    }
}
