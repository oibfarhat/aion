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
    static final int MAX_NET_DELAY = 500; // We can tolerate up to 500ms max delay.

    private final ProcessingTimeService processingTimeService;
    /* Logical division of windows */
    private final long windowLength;
    private final long ssLength;
    private final int ssSize;

    /* Structures to maintain distributions & diststore. */
    private final DistStoreManager<NetDelaySSStore> netDelayStoreManager;
    private final DistStoreManager<GenDelaySSStore> interEventStoreManager;

    private final Map<Long, WindowSSlack> windowSlacksMap;

    /* Stats purger */
    private boolean isWarmedUp;
    private final Thread timestampsPurger;
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

        this.netDelayStoreManager = new DistStoreManager<>(windowLength, ssLength, ssSize);
        this.interEventStoreManager = new DistStoreManager<>(windowLength, ssLength, ssSize);
        this.windowSlacksMap = new HashMap<>();

        /* Purging */
        this.isWarmedUp = false;
        this.timestampsPurger = new Thread(new SSStatsPurger(processingTimeService.getCurrentProcessingTime()));
        this.timestampsPurger.start();
        /* Metrics */
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
                    ssSize,
                    netDist,
                    interEventDist);
            windowSlacksMap.put(windowIndex, ws);
            // Remove from history

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

    final boolean isWarmedUp() {
        return isWarmedUp;
    }

    /* Runnable that purges substreams stats */
    private class SSStatsPurger implements Runnable {

        private long currTime;

        public SSStatsPurger(long currTime) {
            this.currTime = currTime;
        }

        @Override
        public void run() {
            long windowIndex = getWindowIndex(currTime);
            WindowSSlack ws = windowSlacksMap.getOrDefault(windowIndex, null);
            if (ws != null) {
                if (!isWarmedUp) {
                    // We have enough data now!
                    isWarmedUp = true;
                }
                ws.purgeSS(currTime);
            }

            if (windowSlacksMap.containsKey(windowIndex))
                try {
                    Thread.sleep(MAX_NET_DELAY);
                    currTime += MAX_NET_DELAY;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

        }
    }
}
