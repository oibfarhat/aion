package org.apache.flink.streaming.api.operators.watslack.diststore;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.operators.watslack.WindowSSlack;
import org.apache.flink.streaming.api.operators.watslack.WindowSSlackManager;

import java.util.*;

import static org.apache.flink.streaming.api.operators.watslack.WindowSSlackManager.STATS_SIZE;

public class DistStoreManager {

    public enum DistStoreType {
        NET_DELAY,
        GEN_DELAY
    }

    private final WindowSSlackManager sSlackManager;
    private final DistStoreType storeType;
    private final Map<WindowSSlack, WindowDistStore> distStoreByWindow;

    private final Set<SSDistStore> purgedSSSet;
    /* Metrics */
    private final Histogram meanDelayPerSS;

    public DistStoreManager(
            final WindowSSlackManager sSlackManager,
            final DistStoreType storeType) {
        this.sSlackManager = sSlackManager;
        this.storeType = storeType;

        this.distStoreByWindow = new HashMap<>();
        this.purgedSSSet = new TreeSet<>();

        this.meanDelayPerSS = new DescriptiveStatisticsHistogram(STATS_SIZE);
    }

    public WindowDistStore createWindowDistStore(WindowSSlack windowSSlack) {
        WindowDistStore windowStore = new WindowDistStore(windowSSlack, this, sSlackManager.getSSSize());
        distStoreByWindow.put(windowSSlack, windowStore);
        return windowStore;
    }

    /*
     * @return the set of purged substreams
     */
    public Set<SSDistStore> getPurgedData() {
        return purgedSSSet;
    }

    public boolean removeWindowDistStore(WindowSSlack windowSSlack) {
        WindowDistStore store = distStoreByWindow.remove(windowSSlack);
        if (store == null) {
            return false;
        }
        purgedSSSet.removeIf(ssStore -> ssStore.getWindowIndex() == windowSSlack.getWindowIndex());
        return true;
    }

    public void addPurgedSS(SSDistStore purgedSS) {
        purgedSSSet.add(purgedSS);

        meanDelayPerSS.update((long) Math.ceil(purgedSS.getMean()));
    }

    public HistogramStatistics getMeanDelay() {
        return meanDelayPerSS.getStatistics();
    }

    DistStoreType getStoreType() {
        return storeType;
    }

}
