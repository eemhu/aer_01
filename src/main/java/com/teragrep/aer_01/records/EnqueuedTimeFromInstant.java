package com.teragrep.aer_01.records;

import com.teragrep.akv_01.event.metadata.time.EnqueuedTime;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public final class EnqueuedTimeFromInstant implements EnqueuedTime {
    private final Instant instant;

    public EnqueuedTimeFromInstant(final Instant instant) {
        this.instant = instant;
    }

    @Override
    public ZonedDateTime zonedDateTime() {
        return instant.atZone(ZoneId.of("UTC"));
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
