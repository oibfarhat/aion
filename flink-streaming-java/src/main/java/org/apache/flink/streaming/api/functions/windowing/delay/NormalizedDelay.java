package org.apache.flink.streaming.api.functions.windowing.delay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class NormalizedDelay implements StreamDelayStats {

    /** The logger used by this class. */
    protected static final Logger LOG = LoggerFactory.getLogger(NormalizedDelay.class);

    private final List<EpochDelay> epochs;

    private final long watermarkInterval;

    private long earliestTimestamp;
    private long lastEpochStart;


    public NormalizedDelay(final long watermarkInterval) {
        this.epochs = new LinkedList<>();

        this.watermarkInterval = watermarkInterval;
        this.earliestTimestamp = 0;
    }

    @Override
    public void collectDelay(long timestamp, long delay) {
        // collect earliest timestamp
        if (this.earliestTimestamp == 0) {
            this.earliestTimestamp = timestamp;
            this.lastEpochStart = earliestTimestamp;
        }

        LOG.info("Collected %ld with delay %ld", timestamp, delay);

        // Find a method for faster access
        for (EpochDelay epoch : epochs) {
            if (timestamp >= epoch.epochStart && timestamp <= epoch.epochEnd) {
                epoch.collectDelay(delay);
                return;
            }
        }

        // Create a new epoch
        this.lastEpochStart += watermarkInterval;
        EpochDelay epoch = new EpochDelay(this.lastEpochStart, this.lastEpochStart + watermarkInterval);
        epoch.collectDelay(delay);
        epochs.add(epoch);
    }
}
