package org.apache.flink.streaming.api.operators.watslack;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.operators.watslack.diststore.DistStoreManager;
import org.apache.flink.streaming.api.operators.watslack.diststore.GenDelaySSStore;
import org.apache.flink.streaming.api.operators.watslack.diststore.NetDelaySSStore;
import org.apache.flink.streaming.api.operators.watslack.diststore.WindowDistStore;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.HashMap;
import java.util.Map;

/**
 * This class provides an interface for Source operators to retrieve windows.
 * Internally, this class manages all windows.
 */
public final class WindowSSlackManager {

    private static final int HISTORY_SIZE = 1024;

    private final ProcessingTimeService processingTimeService;
    /* Logical division of windows */
    private final long windowLength;
    private final long ssLength;
    private final int ssSize;

    /* Structures to maintain distributions & diststore. */
    private final DistStoreManager<NetDelaySSStore> netDelayStoreManager;
    private final DistStoreManager<GenDelaySSStore> interEventStoreManager;

    private final Map<Long, WindowSSlack> windowSlacksMap;

    /* Metrics */
    private final Counter windowsCounter;

    public WindowSSlackManager(
            final ProcessingTimeService processingTimeService,
            final long windowLength,
            final long ssLength,
            final int ssSize) {
        this.processingTimeService = processingTimeService;

        this.windowLength = windowLength;
        this.ssLength = ssLength;
        this.ssSize = ssSize;

        this.netDelayStoreManager = new DistStoreManager<>(HISTORY_SIZE, windowLength, ssLength, ssSize);
        this.interEventStoreManager = new DistStoreManager<>(HISTORY_SIZE, windowLength, ssLength, ssSize);

        this.windowSlacksMap = new HashMap<>();
        this.windowsCounter = new SimpleCounter();
    }

    public WindowSSlack getWindowSlack(long eventTime) {
        long windowIndex = getWindowIndex(eventTime);
        WindowSSlack ws = windowSlacksMap.getOrDefault(windowIndex, null);
        // New window!
        if (ws == null) {
            WindowDistStore<NetDelaySSStore> netDist = netDelayStoreManager.createWindowDistStore(windowIndex);
            WindowDistStore<GenDelaySSStore> interEventDist = interEventStoreManager.createWindowDistStore(windowIndex);

            ws = new WindowSSlack(
                    windowIndex,
                    this,
                    null,
                    windowLength,
                    ssLength,
                    netDist,
                    interEventDist);
            windowSlacksMap.put(windowIndex, ws);

            windowsCounter.inc();
        }
        return ws;
    }

    final long getWindowIndex(long eventTime) {
        return (long) Math.ceil(eventTime / (windowLength * 1.0));
    }

    final ProcessingTimeService getProcessingTimeService() {
        return processingTimeService;
    }
}
