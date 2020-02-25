package org.apache.flink.streaming.api.operators.watslack.diststore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetDelaySSStore implements SSDistStore {

    protected static final Logger LOG = LoggerFactory.getLogger(NetDelaySSStore.class);

    private final long ssIndex;
    private double mean;
    private  double sd;
    private long count;
    private boolean isPurged;

    NetDelaySSStore(long ssIndex) {
        this.ssIndex = ssIndex;
        this.mean = 0;
        this.sd = 0;
        this.count = 0;
        this.isPurged = false;
    }

    @Override
    public long getSSIndex() {
        return ssIndex;
    }

    @Override
    public void addValue(long netDelay) {
        if (isPurged) {
            LOG.warn("Attempting to add a value to a purged substream");
            return;
        }
        mean += netDelay;
        sd += (netDelay * netDelay);
        count++;
    }

    @Override
    public void purge() {
        if (this.isPurged) {
            LOG.warn("Attempting to purge an already purged substream");
            return;
        }

        if (this.count > 0) {
            this.mean /= count;
            this.sd = (this.sd / this.count) - (this.mean * this.mean);
        }
        this.isPurged = true;
    }
}
