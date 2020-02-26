package org.apache.flink.streaming.api.operators.watslack;

import org.apache.flink.streaming.api.operators.watslack.diststore.WindowDistStore;
import org.apache.flink.streaming.api.operators.watslack.estimators.SSSizeEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.streaming.api.operators.watslack.WindowSSlackManager.MAX_NET_DELAY;

public class WindowSSlack {

    protected static final Logger LOG = LoggerFactory.getLogger(WindowSSlack.class);

    /* Identifiers for WindowSS */
    private final long windowIndex;
    private final WindowSSlackManager sSlackManager;
    private final long windowLength;
    private final long ssLength;
    private final int ssSize;

    /* Stores */
    private WindowDistStore netDelayStore;
    private WindowDistStore genDelayStore;
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
            final long windowLength,
            final long ssLength,
            final int ssSize,
            /* Stores */
            final WindowDistStore netDelayStore,
            final WindowDistStore genDelayStore) {
        this.windowIndex = windowIndex;
        this.sSlackManager = sSlackManager;
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
        return (int) ((eventTime - (windowIndex * windowLength)) / ssLength);
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

        /* Consider the algorithm's wise opinion. */
        if (sSlackManager.getsSlackAlg().sample(windowIndex, localSSIndex)) {
            sampledEvents[localSSIndex]++;
            return true;
        }
        shedEvents[localSSIndex]++;
        return false;
    }

    /*
     * Public interface that determines to sample the tuple or not.
     *
     * @returns a boolean value that determines if the tuple to be included in the sample.
     */
    public long emitWatermark(long eventTime) {
        int localSSIndex = getSSLocalIndex(eventTime);
        long totalEvents = sampledEvents[localSSIndex] +shedEvents[localSSIndex];
        double ratio = sampledEvents[localSSIndex] / (totalEvents * 1.0);
        return sSlackManager.getsSlackAlg().emitWatermark(windowIndex, localSSIndex, totalEvents, ratio);

    }

    boolean purgeSS(long maxPurgeTime) {
        boolean succPurged = false;
        for (long time = windowIndex * windowLength; time <= maxPurgeTime; time += ssLength) {
            int ssLocalIndex = getSSLocalIndex(maxPurgeTime);
            boolean newlyPurged = netDelayStore.purgeSS(ssLocalIndex) || genDelayStore.purgeSS(ssLocalIndex);
            if (newlyPurged) {
                long observedEvents = sampledEvents[ssLocalIndex] + shedEvents[ssLocalIndex];
                double samplingRatio = sampledEvents[ssLocalIndex] / (observedEvents * 1.0);
                sSlackManager
                        .getsSlackAlg().updateAfterPurging(windowIndex, ssLocalIndex, observedEvents, samplingRatio);

                LOG.info(
                        "Purging {}.{}: [sampled: {}, discarded: {}, total: {}, sampling rate: {}",
                        windowIndex, ssLocalIndex, sampledEvents[ssLocalIndex], shedEvents[ssLocalIndex],
                        observedEvents, samplingRatio);
            }
            succPurged |= newlyPurged;
        }
        return succPurged;
    }

    public long getWindowIndex() {
        return windowIndex;
    }
}
