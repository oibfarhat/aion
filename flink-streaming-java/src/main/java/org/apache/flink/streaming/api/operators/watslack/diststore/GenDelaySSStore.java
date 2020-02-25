package org.apache.flink.streaming.api.operators.watslack.diststore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.PriorityQueue;

public class GenDelaySSStore implements SSDistStore {

    protected static final Logger LOG = LoggerFactory.getLogger(GenDelaySSStore.class);

    private final long ssIndex;
    private final PriorityQueue<Long> eventsQueue;
    private double mean;
    private double sd;
    private long count;
    private boolean isPurged;

    public GenDelaySSStore(final long ssIndex) {
        this.ssIndex = ssIndex;
        this.mean = 0;
        this.sd = 0;
        this.count = 0;
        this.isPurged = false;
        this.eventsQueue = new PriorityQueue<>();
    }

    @Override
    public long getSSIndex() {
        return ssIndex;
    }

    @Override
    public void addValue(long eventTime) {
        if (isPurged) {
            LOG.warn("Attempting to add a value to a purged substream");
            return;
        }
        eventsQueue.add(eventTime);
    }

    @Override
    public void purge() {
        if (this.isPurged) {
            LOG.warn("Attempting to purge an already purged substream");
            return;
        }

        this.isPurged = true;
        if (eventsQueue.isEmpty()) {
            LOG.warn("Purging an empty substream");
            return;
        }

        long lastTs = eventsQueue.poll();
        while (!eventsQueue.isEmpty()) {
            long currTs = eventsQueue.poll();
            long genDelay = currTs - lastTs;
            mean += genDelay;
            sd += (genDelay * genDelay);
            count++;

            lastTs = currTs;
        }

        if (this.count > 0) {
            this.mean /= count;
            this.sd = (this.sd / this.count) - (this.mean * this.mean);
        }
        this.eventsQueue.clear();
    }
}