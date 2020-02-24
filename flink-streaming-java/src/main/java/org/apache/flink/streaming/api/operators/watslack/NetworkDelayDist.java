package org.apache.flink.streaming.api.operators.watslack;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * This class is used to store the mean and standard deviation for each network delay epoch.
 * TODO(oibfarhat): Embed statistics.
 */
public class NetworkDelayDist {

    private final class SubStreamProperties {
        private final long substreamIndex;
        private double mean;
        private double sd;
        private long count;
        private boolean finalized;

        SubStreamProperties(long substreamIndex) {
            this.substreamIndex = substreamIndex;
            this.mean = 0;
            this.sd = 0;
            this.count = 0;
            this.finalized = false;
        }

        SubStreamProperties(long substreamIndex, double mean, double sd, long count) {
            this.substreamIndex = substreamIndex;
            this.mean = mean;
            this.sd = sd;
            this.count = count;
            this.finalized = true;
        }

        void addElement(long networkDelay) {
            if (!finalized) {
                return;
            }
            mean += networkDelay;
            sd += (networkDelay * networkDelay);
            count++;
        }

        void finalizeSubstream() {
            this.finalized = true;
            if (this.count > 0) {
                this.mean /= count;
                this.sd = (this.sd / this.count) - (this.mean * this.mean);
            }
        }
    }

    /* Size of substream info to maintain. */
    private final int historySize;
    /* Data structures to preserve historical information. */
    private final LinkedList<SubStreamProperties> historicalSubstreams;
    /* Running substreams properties */
    private final Map<Long, SubStreamProperties> currSubstreams;
    /* Last finalized substream. */
    private long substreamWatermark;

    public NetworkDelayDist(int historySize) {
        this.historySize = historySize;
        /* Data Structures */
        this.historicalSubstreams = new LinkedList<>();
        this.currSubstreams = new HashMap<>();

        this.substreamWatermark = 0;
    }

    /*
     * Public interface for the user to add a specific delay.
     */
    public void addDelay(long substreamIndex, long delay) {
        SubStreamProperties subStreamProperties = currSubstreams.getOrDefault(substreamIndex, null);

        // New substream!!
        if (subStreamProperties == null) {
            subStreamProperties = new SubStreamProperties(substreamIndex);
            currSubstreams.put(substreamIndex, subStreamProperties);
        }

        subStreamProperties.addElement(delay);
    }

    /*
     * Public interface for the user to signal an end of a substream
     */
    public void finalizeSubstream(long substreamIndex) {
        SubStreamProperties props = currSubstreams.remove(substreamIndex);
        if (props == null) {
            // TODO(oibfarhat): Add logger
            return;
        }

        props.finalizeSubstream();
        historicalSubstreams.addLast(props);
        while (historicalSubstreams.size() >= historySize) {
            historicalSubstreams.removeFirst();
        }
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

        SubStreamProperties estimatedProperties = new SubStreamProperties(substreamIndex);
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

