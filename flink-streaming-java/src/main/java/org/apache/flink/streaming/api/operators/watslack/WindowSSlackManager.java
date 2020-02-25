package org.apache.flink.streaming.api.operators.watslack;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.operators.watslack.diststore.DistStoreManager;
import org.apache.flink.streaming.api.operators.watslack.diststore.WindowDistStore;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.HashMap;
import java.util.Map;

/**
 * This class provides an interface for Source operators to retrieve windows.
 * Internally, this class manages all windows.
 */
public final class WindowSSlackManager {

    private final ProcessingTimeService processingTimeService;

    /* Logical division of windows */
    private final long windowSize;
    private final long ssSize;

    /* Structures to maintain distributions & diststore. */
    private final DistStoreManager netDelayStoreManager;
    private final DistStoreManager interEventStoreManager;

    private final Map<Long, WindowSSlack> windowSlacksMap;

    /* Metrics */
    private final Counter windowsCounter;

    public WindowSSlackManager(
            ProcessingTimeService processingTimeService,
            long windowSize, long ssSize) {
        this.processingTimeService = processingTimeService;

        this.windowSize = windowSize;
        this.ssSize = ssSize;

        this.netDelayStoreManager = new DistStoreManager(1024);
        this.interEventStoreManager = new DistStoreManager(1024);

        this.windowSlacksMap = new HashMap<>();
        this.windowsCounter = new SimpleCounter();
    }

    public WindowSSlack getWindowSlack(long eventTime) {
        long windowIndex = getWindowIndex(eventTime);
        WindowSSlack ws = windowSlacksMap.getOrDefault(windowIndex, null);
        // New window!
        if (ws == null) {
            WindowDistStore netDist = netDelayStoreManager.createWindowDistStore(windowIndex);
            WindowDistStore interEventDist = interEventStoreManager.createWindowDistStore(windowIndex);

            ws = new WindowSSlack(
                    windowIndex, this, null, windowSize, ssSize, netDist, interEventDist, );
            windowSlacksMap.put(windowIndex, ws);

            windowsCounter.inc();
        }
        return ws;
    }

    final long getWindowIndex(long eventTime) {
        return (long) Math.ceil(eventTime / (windowSize * 1.0));
    }

    final ProcessingTimeService getProcessingTimeService() {
        return processingTimeService;
    }
}
