package org.apache.flink.streaming.api.operators.watslack;

import org.apache.flink.streaming.api.operators.watslack.diststore.WindowDistStore;
import org.apache.flink.streaming.api.operators.watslack.sampling.SamplingSlackAlg;

public class WindowSSlack {

    /* Identifiers for WindowSS */
    private final long windowIndex;
    private final WindowSSlackManager sSlackManager;
    private final SamplingSlackAlg samplingSlackAlg;
    private final long windowSize;
    private final long ssSize;

    /* Estimators */
    private WindowDistStore netDelayStore;
    private WindowDistStore genDelayStore;
//    private final SSSizeEstimator ssSizeEstimator;
//    private final SSSampleSizeEstimator ssSampleSizeEstimator;

    /* Book-keeping data structures . */
    private final long[] sampledEvents;
    private final long[] shedEvents;
    private final boolean[] watermarkEmitted;


    WindowSSlack(
            /* Identifiers */
            final long windowIndex,
            final WindowSSlackManager sSlackManager,
            final SamplingSlackAlg samplingSlackAlg,
            final long windowSize,
            final long ssSize,
            /* Estimators */
            final WindowDistStore netDelayStore,
            final WindowDistStore genDelayStore) {
        this.windowIndex = windowIndex;
        this.sSlackManager = sSlackManager;
        this.samplingSlackAlg = samplingSlackAlg;
        this.windowSize = windowSize;
        this.ssSize = ssSize;

        this.netDelayStore = netDelayStore;
        this.genDelayStore = genDelayStore;

        int numOfSS = (int) Math.ceil(windowSize / (ssSize * 1.0));
        sampledEvents = new long[numOfSS];
        shedEvents = new long[numOfSS];
//        targetSamplingRate = new long[numOfSS];
    }

    /*
     * Internal function that @returns local substream index in relation to window.
     */
    private int getSSLocalIndex(long eventTime) {
        assert sSlackManager.getWindowIndex(eventTime) == windowIndex;
        return (int) ((windowIndex * windowSize) % ssSize);
    }

    /*
     * Public interface that determines to sample the tuple or not.
     *
     * @returns a boolean value that determines if the tuple to be included in the sample.
     */
    public boolean sample(long eventTime) {
        int localSSIndex = getSSLocalIndex(eventTime);
        netDelayStore.addEvent(localSSIndex, eventTime);
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

    public void addEvent(long eventTime) {
        int localSSIndex = getSSLocalIndex(eventTime);
        sampledEvents[localSSIndex]++;
    }

    public void discardEvent(long eventTime) {
        int localSSIndex = getSSLocalIndex(eventTime);
        shedEvents[localSSIndex]++;
    }


}
