package org.apache.flink.streaming.api.operators.watslack.diststore;

import java.util.HashMap;
import java.util.Map;

public class DistStoreManager<T extends SSDistStore> {

    private final long windowLength;
    private final long ssLength;
    private final int ssSize;

    private final Map<Long, WindowDistStore<T>> distStoreByWindow;

    public DistStoreManager(
            final long windowLength,
            final long ssLength,
            final int ssSize) {
        this.windowLength = windowLength;
        this.ssLength = ssLength;
        this.ssSize = ssSize;

        this.distStoreByWindow = new HashMap<>();
    }

    public WindowDistStore<T> createWindowDistStore(long windowIndex) {
        WindowDistStore<T> windowStore = new WindowDistStore<>(windowIndex, ssSize);
        distStoreByWindow.put(windowIndex, windowStore);
        return windowStore;
    }

    public boolean removeWindowDistStore(long windowIndex) {
        return distStoreByWindow.remove(windowIndex) != null;
    }
}
