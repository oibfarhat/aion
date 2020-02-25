package org.apache.flink.streaming.api.operators.watslack.diststore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetDelaySSStore implements SSDistStore {

    protected static final Logger LOG = LoggerFactory.getLogger(SSDistStore.class);

    private final long ssIndex;
    private boolean finalized;

    protected double mean;
    protected double sd;
    protected long count;

    NetDelaySSStore(long ssIndex) {
        this.ssIndex = ssIndex;
        this.mean = 0;
        this.sd = 0;
        this.count = 0;
        this.finalized = false;
    }


    @Override
    public long getSSIndex() {
        return ssIndex;
    }

    @Override
    public void addValue(long netDelay) {
        mean += netDelay;
        sd += (netDelay * netDelay);
        count++;
    }

    void finalizeSubstream() {
        this.finalized = true;
        if (this.count > 0) {
            this.mean /= count;
            this.sd = (this.sd / this.count) - (this.mean * this.mean);
        }
    }
}
