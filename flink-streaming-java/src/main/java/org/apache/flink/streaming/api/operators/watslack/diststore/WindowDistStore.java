package org.apache.flink.streaming.api.operators.watslack.diststore;

public class WindowDistStore {

    private final long windowIndex;
    private final SSDistStore[] ssStores;
    /* Purging Specific */
    private final boolean[] isSSPurged;
    private DistStoreManager distStoreManager;

    WindowDistStore(
            final long windowIndex,
            final DistStoreManager distStoreManager,
            final int ssSize) {
        this.windowIndex = windowIndex;
        this.distStoreManager = distStoreManager;
        this.isSSPurged = new boolean[ssSize];

        this.ssStores = new SSDistStore[ssSize];
        for (int i = 0; i < ssSize; i++) {
            ssStores[i] =
                    distStoreManager.getStoreType() == DistStoreManager.DistStoreType.NET_DELAY ?
                            new NetDelaySSStore(windowIndex, i) : new GenDelaySSStore(windowIndex, i);
        }
    }

    public void addEvent(int ssLocalIndex, long value) {
        ssStores[ssLocalIndex].addValue(value);
    }

    public boolean purgeSS(int ssLocalIndex) {
        if (!isSSPurged[ssLocalIndex]) {
            ssStores[ssLocalIndex].purge();
            isSSPurged[ssLocalIndex] = true;
            return true;
        }
        return false;
    }
}
