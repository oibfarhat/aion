package org.apache.flink.streaming.api.operators.watslack.diststore;

public class WindowDistStore<T extends SSDistStore> {

    private final long windowIndex;
    private final T[] ssStores;
    /* Purging Specific */
    private final boolean[] isSSPurged;

    public WindowDistStore(
            final long windowIndex,
            final int ssSize) {
        this.windowIndex = windowIndex;

        ssStores = (T[]) new SSDistStore[ssSize];
        isSSPurged = new boolean[ssSize];
    }

    public void addEvent(int ssLocalIndex, long value) {
        ssStores[ssLocalIndex].addValue(value);
    }

    public void purgeSS(int ssLocalIndex) {
        if (!isSSPurged[ssLocalIndex]) {
            ssStores[ssLocalIndex].purge();
            isSSPurged[ssLocalIndex] = true;
        }
    }
}
