package org.apache.flink.streaming.api.operators.watslack;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.operators.watslack.diststore.DistStoreManager;
import org.apache.flink.streaming.api.operators.watslack.diststore.WindowDistStore;
import org.apache.flink.streaming.api.operators.watslack.estimators.WindowSizeEstimator;
import org.apache.flink.streaming.api.operators.watslack.sampling.AbstractSSlackAlg;
import org.apache.flink.streaming.api.operators.watslack.sampling.NaiveSSlackAlg;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.api.operators.watslack.diststore.DistStoreManager.DistStoreType.GEN_DELAY;
import static org.apache.flink.streaming.api.operators.watslack.diststore.DistStoreManager.DistStoreType.NET_DELAY;


/**
 * This class provides an interface for Source operators to retrieve windows.
 * Internally, this class manages all windows.
 */
public final class WindowSSlackManager {

    protected static final Logger LOG = LoggerFactory.getLogger(WindowSSlackManager.class);

    static final int MAX_NET_DELAY = 1000; // We can tolerate up to 500ms max delay.
    private static final int HISTORY_SIZE = 1024;

    private final ProcessingTimeService processingTimeService;
    private final AbstractSSlackAlg sSlackAlg;
    /* Logical division of windows */
    private final long windowLength;
    private final long ssLength;
    private final int ssSize;
    /* Watermarks. */
    private long lastEmittedWatermark = Long.MIN_VALUE;
    /* Structures to maintain distributions & diststore. */
    private final DistStoreManager netDelayStoreManager;
    private final DistStoreManager interEventStoreManager;

    private final Map<Long, WindowSSlack> windowSlacksMap;
    /* Metrics */
    private final Counter windowsCounter;
    /* Stats purger */
    private final Thread timestampsPurger;
    private boolean isWarmedUp;

    public WindowSSlackManager(
            final ProcessingTimeService processingTimeService,
            final long windowLength,
            final long ssLength,
            final int ssSize) {
        this.processingTimeService = processingTimeService;

        this.windowLength = windowLength;
        this.ssLength = ssLength;
        this.ssSize = ssSize;

        this.netDelayStoreManager = new DistStoreManager(this, NET_DELAY);
        this.interEventStoreManager = new DistStoreManager(this, GEN_DELAY);

        WindowSizeEstimator srEstimator =
                new WindowSizeEstimator(this, netDelayStoreManager, interEventStoreManager);
        this.sSlackAlg = new NaiveSSlackAlg(this, srEstimator);

        this.windowSlacksMap = new HashMap<>();

        /* Purging */
        this.isWarmedUp = false;
        this.timestampsPurger = new Thread(new SSStatsPurger(processingTimeService.getCurrentProcessingTime()));
        this.timestampsPurger.start();
        /* Metrics */
        this.windowsCounter = new SimpleCounter();
    }

    /* Getters & Setters */
    public long getWindowLength() {
        return windowLength;
    }

    public long getSSLength() {
        return ssLength;
    }

    public int getSSSize() {
        return ssSize;
    }

    public ProcessingTimeService getProcessingTimeService() {
        return processingTimeService;
    }

    public boolean isWarmedUp() {
        return isWarmedUp;
    }

    public long getLastEmittedWatermark() {
        return lastEmittedWatermark;
    }

    public void setLastEmittedWatermark(long targetWatermark) {
        lastEmittedWatermark = targetWatermark;
    }

    public WindowSSlack getWindowSSLack(long windowIndex) {
        return windowSlacksMap.getOrDefault(windowIndex, null);
    }

    public AbstractSSlackAlg getsSlackAlg() {
        return sSlackAlg;
    }

    DistStoreManager getNetDelayStoreManager() {
        return netDelayStoreManager;
    }

    DistStoreManager getInterEventStoreManager() {
        return interEventStoreManager;
    }

    /* Map manipulation */
    public WindowSSlack getWindowSlack(long eventTime) {
        long windowIndex = getWindowIndex(eventTime);
        WindowSSlack ws = windowSlacksMap.getOrDefault(windowIndex, null);
        // New window!
        if (ws == null) {
            ws = new WindowSSlack(
                    this,
                    windowIndex);
            windowSlacksMap.put(windowIndex, ws);
            sSlackAlg.initWindow(ws);
            // Remove from history
            removeWindowSSlack(windowIndex - HISTORY_SIZE);
            windowsCounter.inc();
        }
        return ws;
    }

    private void removeWindowSSlack(long windowIndex) {
        WindowSSlack window = windowSlacksMap.remove(windowIndex - HISTORY_SIZE);
        if (window != null) {
            LOG.info("Removing window slack {}", windowIndex);
            // remove
        }
    }

    /* Index & Deadlines calculation */
    final long getWindowIndex(long eventTime) {
        return (long) Math.floor(eventTime / (windowLength * 1.0));
    }

    public long getWindowDeadline(long windowIndex) {
        return (windowIndex + 1) * windowLength;
    }

    public long getSSDeadline(long windowIndex, long ssIndex) {
        return windowIndex * windowLength + (ssIndex + 1) * ssLength;
    }

    /* Purging Runnable that purges substreams stats */
    private class SSStatsPurger implements Runnable {

        private long currTime;

        SSStatsPurger(long currTime) {
            this.currTime = currTime;
        }

        @Override
        public void run() {
            sleep(10 * MAX_NET_DELAY); // essential

            while (true) {
                long windowIndex = getWindowIndex(currTime);

                // TODO(oibfarhat): Consider making this more efficient
                for (long currIndex = windowIndex - 15; currIndex <= windowIndex; currIndex++) {
                    WindowSSlack ws = windowSlacksMap.getOrDefault(windowIndex, null);
                    if (ws != null) {
                        if (ws.purgeSS(currTime) && !isWarmedUp) {
                            // We have enough data now!
                            LOG.info("It is finally warmed up at t = {}", currTime);
                            isWarmedUp = true;
                        }
                    }
                }

                sleep(MAX_NET_DELAY);
                currTime += MAX_NET_DELAY;
            }
        }

        public void sleep(int delay) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
