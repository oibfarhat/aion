package org.apache.flink.streaming.api.operators.watslack.estimators;

import org.apache.flink.streaming.api.operators.watslack.WindowSSlackManager;
import org.apache.flink.streaming.api.operators.watslack.diststore.DistStoreManager;
import org.apache.flink.streaming.api.operators.watslack.diststore.SSDistStore;
import org.apache.flink.streaming.api.operators.watslack.sampling.SamplingPlan;

import java.util.Set;

/**
 * This class is responsible to estimate two main attributes:
 * 1) The target sampling rate for the window.
 * 2) The anticipated number of events in each substream.
 */

public class WindowSizeEstimator {

    private final WindowSSlackManager sSlackManager;
    private final DistStoreManager netDelayManager;
    private final DistStoreManager genDelayManager;

    public WindowSizeEstimator(
            final WindowSSlackManager sSlackManager,
            final DistStoreManager netDelayManager,
            final DistStoreManager genDelayManager) {
        this.sSlackManager = sSlackManager;
        this.netDelayManager = netDelayManager;
        this.genDelayManager = genDelayManager;
    }

    public long getEventsNumPerSS(SamplingPlan plan) {
        Set<SSDistStore> purgedNetDelay = netDelayManager.getPurgedData();
        Set<SSDistStore> purgedGenDelay = genDelayManager.getPurgedData();

        assert purgedGenDelay.size() == purgedNetDelay.size();

        double averageNetMean = purgedNetDelay.stream().mapToDouble(SSDistStore::getMean).average().getAsDouble();
        double averageGenMean = purgedGenDelay.stream().mapToDouble(SSDistStore::getMean).average().getAsDouble();

        return (long) Math.ceil((averageNetMean + sSlackManager.getSSLength()) * averageGenMean);
    }
}
