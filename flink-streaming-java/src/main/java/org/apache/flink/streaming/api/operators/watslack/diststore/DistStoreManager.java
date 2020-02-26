package org.apache.flink.streaming.api.operators.watslack.diststore;

import java.util.*;

public class DistStoreManager {

    public enum DistStoreType {
        NET_DELAY,
        GEN_DELAY
    }

    private final long windowLength;
    private final long ssLength;
    private final int ssSize;

    private final DistStoreType storeType;
    private final Map<Long, WindowDistStore> distStoreByWindow;

    private final Set<SSDistStore> purgedSSSet;

    public DistStoreManager(
            final long windowLength,
            final long ssLength,
            final int ssSize,
            final DistStoreType storeType) {
        this.windowLength = windowLength;
        this.ssLength = ssLength;
        this.ssSize = ssSize;
        this.storeType = storeType;

        this.distStoreByWindow = new HashMap<>();
        this.purgedSSSet = new TreeSet<>();
    }

    public WindowDistStore createWindowDistStore(long windowIndex) {
        WindowDistStore windowStore = new WindowDistStore(windowIndex, this, ssSize);
        distStoreByWindow.put(windowIndex, windowStore);
        return windowStore;
    }

    /*
     * @return the set of purged substreams
     */
    public Set<SSDistStore> getPurgedData() {
        return purgedSSSet;
    }

    public boolean removeWindowDistStore(long windowIndex) {
        WindowDistStore store = distStoreByWindow.remove(windowIndex);
        if (store == null) {
            return false;
        }
        purgedSSSet.removeIf(ssStore -> ssStore.getWindowIndex() == windowIndex);
        return true;
    }

    public void addPurgedSS(SSDistStore purgedSS) {
        purgedSSSet.add(purgedSS);
    }

    DistStoreType getStoreType() {
        return storeType;
    }

}
