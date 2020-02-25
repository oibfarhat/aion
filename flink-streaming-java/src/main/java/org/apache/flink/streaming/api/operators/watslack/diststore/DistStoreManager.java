package org.apache.flink.streaming.api.operators.watslack.diststore;

import java.util.HashMap;
import java.util.Map;

public class DistStoreManager<T extends > {
    private final long historySize;

    private final Map<Long, WindowDistStore> distStoreByWindow;

    public DistStoreManager(
            final long historySize) {
        this.historySize = historySize;

        this.distStoreByWindow = new HashMap<>();
    }

    public WindowDistStore createWindowDistStore(long windowIndex) {
        WindowDistStore windowStore = new WindowDistStore(windowIndex);
        distStoreByWindow.put(windowIndex, windowStore);

        // Remove the window size of windowIndex - historySize
        distStoreByWindow.remove(windowIndex - historySize);
        return windowStore;
    }

    public pullHistory() {

    }
}
