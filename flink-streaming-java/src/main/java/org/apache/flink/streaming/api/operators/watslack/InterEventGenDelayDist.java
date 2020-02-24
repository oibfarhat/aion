package org.apache.flink.streaming.api.operators.watslack;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * This class is used to store the mean and standard deviation for event inter-gen delay.
 * TODO(oibfarhat): Embed statistics.
 */
public class InterEventGenDelayDist {

    private final class SubStreamProperties {
        private final long substreamIndex;
        private final double mean;
        private final double sd;
        private final long count;

        SubStreamProperties(long substreamIndex, double mean, double sd, long count) {
            this.substreamIndex = substreamIndex;
            this.mean = mean;
            this.sd = sd;
            this.count = count;
        }
    }

    /* Size of substream info to maintain. */
    private final int historySize;
    /* Data structures to preserve historical information. */
    private final LinkedList<SubStreamProperties> historicalSubstreams;
    /* Running substreams properties */
    private final Map<Long, PriorityQueue<Long>> currSubstreams;
    /* Last finalized substream. */
    private long substreamWatermark;

    public InterEventGenDelayDist(int historySize) {
        this.historySize = historySize;
        /* Data Structures */
        this.historicalSubstreams = new LinkedList<>();
        this.currSubstreams = new HashMap<>();

        this.substreamWatermark = 0;
    }

    /*
     * Public interface for the user to add a specific delay.
     */
    public void addGenEvent(long substreamIndex, long genTime) {
        PriorityQueue<Long> pQueue = currSubstreams.getOrDefault(substreamIndex, null);

        // New substream!!
        if (pQueue == null) {
            pQueue = new PriorityQueue<>();
            currSubstreams.put(substreamIndex, pQueue);
        }

        pQueue.add(genTime);
    }

    /*
     * Public interface for the user to signal an end of a substream
     */
    public void finalizeSubstream(long substreamIndex) {
        PriorityQueue<Long> pQueue = currSubstreams.remove(substreamIndex);
        if (pQueue == null)
            return;

        SubStreamProperties props;
        if (pQueue.isEmpty()) {
            props = new SubStreamProperties(substreamIndex, 0, 0, 0);
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

            props = new SubStreamProperties(substreamIndex, runningMean, runningSD, size);
        }
        // Add to history
        historicalSubstreams.addLast(props);
        while (historicalSubstreams.size() >= historySize) {
            historicalSubstreams.removeFirst();
        }
        // Remove from Map
        pQueue.clear();

        this.substreamWatermark = Math.max(this.substreamWatermark, substreamIndex);
    }

    /*
     * Summarize substream
     */
    public SubStreamProperties estimateSubstream(long substreamIndex) {
        // Pull from history
        if (this.substreamWatermark > substreamIndex) {
            for (SubStreamProperties subStreamProperties : historicalSubstreams)
                if (subStreamProperties.substreamIndex == substreamIndex)
                    return subStreamProperties;
            // Element not found, too old
            return null;
        }

        double mean = 0;
        double sd = 0;
        long count = 0;
        for (SubStreamProperties historicalSubStream : historicalSubstreams) {
            mean += historicalSubStream.mean;
            sd += historicalSubStream.sd;
            count++;
        }
        return new SubStreamProperties(substreamIndex, mean / count, sd / count, count);
    }
}

