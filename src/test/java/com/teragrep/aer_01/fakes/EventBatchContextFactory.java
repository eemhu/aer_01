package com.teragrep.aer_01.fakes;

import com.azure.messaging.eventhubs.models.EventBatchContext;

public interface EventBatchContextFactory {
    public abstract EventBatchContext eventBatchContext();
}
