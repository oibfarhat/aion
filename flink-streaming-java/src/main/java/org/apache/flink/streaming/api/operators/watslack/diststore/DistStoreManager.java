package org.apache.flink.streaming.api.operators.watslack.diststore;

import java.util.HashMap;
import java.util.Map;

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
    }

    public WindowDistStore createWindowDistStore(long windowIndex) {
        WindowDistStore windowStore = new WindowDistStore(windowIndex, this, ssSize);
        distStoreByWindow.put(windowIndex, windowStore);
        return windowStore;
    }

    public boolean removeWindowDistStore(long windowIndex) {
        return distStoreByWindow.remove(windowIndex) != null;
    }

    DistStoreType getStoreType() {
        return storeType;
    }

}
