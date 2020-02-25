package org.apache.flink.streaming.api.operators.watslack.garbage;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * This class is used to store the mean and standard deviation for event inter-gen delay.
 * TODO(oibfarhat): Embed statistics.
 */
public class InterEventGenDelayDist {

    final class SSGenProp {
        private final long ssIndex;
        final double mean;
        final double sd;
        private final long count;

        SSGenProp(long ssIndex, double mean, double sd, long count) {
            this.ssIndex = ssIndex;
            this.mean = mean;
            this.sd = sd;
            this.count = count;
        }
    }

    /* Size of substream info to maintain. */
    private final int historySize;
    /* Data structures to preserve historical information. */
    private final LinkedList<SSGenProp> historicalSSList;
    /* Running substreams properties */
    private final Map<Long, PriorityQueue<Long>> runningSSMap;
    /* Last finalized substream. */
    private long substreamWatermark;

    public InterEventGenDelayDist(int historySize) {
        this.historySize = historySize;
        /* Data Structures */
        this.historicalSSList = new LinkedList<>();
        this.runningSSMap = new HashMap<>();

        this.substreamWatermark = 0;
    }

    /*
     * Public interface for the user to add a specific delay.
     */
    public void add(long substreamIndex, long genTime) {
        PriorityQueue<Long> pQueue = runningSSMap.getOrDefault(substreamIndex, null);

        // New substream!!
        if (pQueue == null) {
            pQueue = new PriorityQueue<>();
            runningSSMap.put(substreamIndex, pQueue);
        }

        pQueue.add(genTime);
    }

    /*
     * Public interface for the user to signal an end of a substream
     */
    public void finalize(long ssIndex) {
        PriorityQueue<Long> pQueue = runningSSMap.remove(ssIndex);
        if (pQueue == null)
            return;

        SSGenProp props;
        if (pQueue.isEmpty()) {
            props = new SSGenProp(ssIndex, 0, 0, 0);
        } else {
            double runningMean = 0;
            double runningSD = 0;
            long size = 0;

            long lastTS = pQueue.poll();

            while (!pQueue.isEmpty()) {
                long currTS = pQueue.poll();
                long genDelay = currTS - lastTS;
                runningMean += genDelay;
                runningSD += (genDelay * genDelay);
                size++;

                lastTS = currTS;
            }

            props = new SSGenProp(ssIndex, runningMean, runningSD, size);
        }
        // Add to history
        historicalSSList.addLast(props);
        while (historicalSSList.size() >= historySize) {
            historicalSSList.removeFirst();
        }
        // Remove from Map
        pQueue.clear();

        this.substreamWatermark = Math.max(this.substreamWatermark, ssIndex);
    }

    /*
     * Summarize substream
     */
    public SSGenProp estimate(long ssIndex) {
        // Pull from history
        if (this.substreamWatermark > ssIndex) {
            for (SSGenProp subStreamProperties : historicalSSList)
                if (subStreamProperties.ssIndex == ssIndex)
                    return subStreamProperties;
            // Element not found, too old
            return null;
        }

        double mean = 0;
        double sd = 0;
        long count = 0;
        for (SSGenProp historicalSubStream : historicalSSList) {
            mean += historicalSubStream.mean;
            sd += historicalSubStream.sd;
            count++;
        }
        return new SSGenProp(ssIndex, mean / count, sd / count, count);
    }
}

