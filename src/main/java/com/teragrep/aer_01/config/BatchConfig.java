package com.teragrep.aer_01.config;

import com.teragrep.aer_01.config.source.Sourceable;

public final class BatchConfig {
    private final int maxBatchSize;

    public BatchConfig(final Sourceable sourceable) {
        this(Integer.parseInt(sourceable.source("batch.max.size", "1000")));
    }

    public BatchConfig(final int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public int maxBatchSize() {
        return maxBatchSize;
    }
}
