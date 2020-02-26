package org.apache.flink.streaming.api.operators.watslack.diststore;

import org.apache.flink.streaming.api.operators.watslack.WindowSSlack;
import org.apache.flink.streaming.api.operators.watslack.WindowSSlackManager;

import java.util.*;

public class DistStoreManager {

    public enum DistStoreType {
        NET_DELAY,
        GEN_DELAY
    }

    private final WindowSSlackManager sSlackManager;
    private final DistStoreType storeType;
    private final Map<WindowSSlack, WindowDistStore> distStoreByWindow;

    private final Set<SSDistStore> purgedSSSet;

    public DistStoreManager(
            final WindowSSlackManager sSlackManager,
            final DistStoreType storeType) {
        this.sSlackManager = sSlackManager;
        this.storeType = storeType;

        this.distStoreByWindow = new HashMap<>();
        this.purgedSSSet = new TreeSet<>();
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
    }

    DistStoreType getStoreType() {
        return storeType;
    }

}
