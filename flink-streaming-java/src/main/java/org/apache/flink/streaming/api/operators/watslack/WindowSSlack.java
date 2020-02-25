package org.apache.flink.streaming.api.operators.watslack;

import org.apache.flink.streaming.api.operators.watslack.diststore.GenDelaySSStore;
import org.apache.flink.streaming.api.operators.watslack.diststore.NetDelaySSStore;
import org.apache.flink.streaming.api.operators.watslack.diststore.WindowDistStore;
import org.apache.flink.streaming.api.operators.watslack.estimators.SSSizeEstimator;
import org.apache.flink.streaming.api.operators.watslack.sampling.AbstractSSlackAlg;

public class WindowSSlack {

    private static final int MAX_NET_DELAY = 1000; // We can tolerate up to 1s max delay.

    /* Identifiers for WindowSS */
    private final long windowIndex;
    private final WindowSSlackManager sSlackManager;
    private final AbstractSSlackAlg samplingSlackAlg;
    private final long windowLength;
    private final long ssLength;
    /* Stores */
    private WindowDistStore<NetDelaySSStore> netDelayStore;
    private WindowDistStore<GenDelaySSStore> genDelayStore;
    /* Estimators */
    private SSSizeEstimator[] sssEstimator;
    /*
     * Book-keeping data structures.
     * TODO(oibfarhat): I dont feel this is the right place for these data structures.
     */
    private final long[] sampledEvents;
    private final long[] shedEvents;

    WindowSSlack(
            /* Identifiers */
            final long windowIndex,
            final WindowSSlackManager sSlackManager,
            final AbstractSSlackAlg samplingSlackAlg,
            final long windowLength,
            final long ssLength,
            /* Stores */
            final WindowDistStore<NetDelaySSStore> netDelayStore,
            final WindowDistStore<GenDelaySSStore> genDelayStore,
            /* Estimators */
            final SSSizeEstimator sssEstimator) {
        this.windowIndex = windowIndex;
        this.sSlackManager = sSlackManager;
        this.samplingSlackAlg = samplingSlackAlg;
        this.windowLength = windowLength;
        this.ssLength = ssLength;

        this.netDelayStore = netDelayStore;
        this.genDelayStore = genDelayStore;
        this.sssEstimator = sssEstimator;

        int ssSize = (int) Math.ceil(windowLength / (ssLength * 1.0));
        sampledEvents = new long[ssSize];
        shedEvents = new long[ssSize];
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

        // In such cases, we do not bookeep such delays.
        if (delay > MAX_NET_DELAY) {
            return false;
        }

        netDelayStore.addEvent(localSSIndex, delay);
        genDelayStore.addEvent(localSSIndex, eventTime);

        if (samplingSlackAlg.sample()) {
            sampledEvents[localSSIndex]++;
            return true;
        }

        shedEvents[localSSIndex]++;
        return false;
    }

    public long getWindowIndex() {
        return windowIndex;
    }
}
