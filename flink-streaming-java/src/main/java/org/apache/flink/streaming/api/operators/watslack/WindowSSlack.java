package org.apache.flink.streaming.api.operators.watslack;

import org.apache.flink.streaming.api.operators.watslack.diststore.GenDelaySSStore;
import org.apache.flink.streaming.api.operators.watslack.diststore.NetDelaySSStore;
import org.apache.flink.streaming.api.operators.watslack.diststore.WindowDistStore;
import org.apache.flink.streaming.api.operators.watslack.estimators.SSSizeEstimator;
import org.apache.flink.streaming.api.operators.watslack.sampling.AbstractSSlackAlg;

import static org.apache.flink.streaming.api.operators.watslack.WindowSSlackManager.MAX_NET_DELAY;

public class WindowSSlack {

    /* Identifiers for WindowSS */
    private final long windowIndex;
    private final WindowSSlackManager sSlackManager;
    private final AbstractSSlackAlg samplingSlackAlg;
    private final long windowLength;
    private final long ssLength;
    private final int ssSize;
    /*
     * Book-keeping data structures.
     * TODO(oibfarhat): I dont feel this is the right place for these data structures.
     */
    private final long[] sampledEvents;
    private final long[] shedEvents;
    /* Stores */
    private WindowDistStore<NetDelaySSStore> netDelayStore;
    private WindowDistStore<GenDelaySSStore> genDelayStore;
    /* Estimators */
    private SSSizeEstimator[] sssEstimator;

    WindowSSlack(
            /* Identifiers */
            final long windowIndex,
            final WindowSSlackManager sSlackManager,
            final AbstractSSlackAlg samplingSlackAlg,
            final long windowLength,
            final long ssLength,
            final int ssSize,
            /* Stores */
            final WindowDistStore<NetDelaySSStore> netDelayStore,
            final WindowDistStore<GenDelaySSStore> genDelayStore) {
        this.windowIndex = windowIndex;
        this.sSlackManager = sSlackManager;
        this.samplingSlackAlg = samplingSlackAlg;
        this.windowLength = windowLength;
        this.ssLength = ssLength;
        this.ssSize = ssSize;

        this.netDelayStore = netDelayStore;
        this.genDelayStore = genDelayStore;

        this.sampledEvents = new long[ssSize];
        this.shedEvents = new long[ssSize];
    }

    /*
     * Internal function that @returns local substream index in relation to window.
     */
    private int getSSLocalIndex(long eventTime) {
        assert sSlackManager.getWindowIndex(eventTime) == windowIndex;
        return (int) ((windowIndex * windowLength) % ssLength);
    }

    /*
     * Public interface that determines to sample the tuple or not.
     *
     * @returns a boolean value that determines if the tuple to be included in the sample.
     */
    public boolean sample(long eventTime) {
        int localSSIndex = getSSLocalIndex(eventTime);
        long delay = sSlackManager.getProcessingTimeService().getCurrentProcessingTime() - eventTime;

        /* In the case of extreme network delay, we do not consider such events. */
        if (delay > MAX_NET_DELAY) {
            return false;
        }

        netDelayStore.addEvent(localSSIndex, delay);
        genDelayStore.addEvent(localSSIndex, eventTime);

        /* If we still don't have sufficient data */
        if (sSlackManager.isWarmedUp()) {
            sampledEvents[localSSIndex]++;
            return true;
        }

        /* Consider the algorithm's wise opinion. */
        if (samplingSlackAlg.sample()) {
            sampledEvents[localSSIndex]++;
            return true;
        }

        shedEvents[localSSIndex]++;
        return false;
    }

    void purgeSS(long maxPurgeTime) {
        for (long time = windowIndex * windowLength; time <= maxPurgeTime; time += ssLength) {
            int ssLocalIndex = getSSLocalIndex(maxPurgeTime);
            netDelayStore.purgeSS(ssLocalIndex);
            genDelayStore.purgeSS(ssLocalIndex);
        }
    }

    public long getWindowIndex() {
        return windowIndex;
    }
}
