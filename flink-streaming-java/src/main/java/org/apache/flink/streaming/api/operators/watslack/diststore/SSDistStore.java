package org.apache.flink.streaming.api.operators.watslack.diststore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to store the mean and standard deviation for each diststore delay substream (ss).
 * TODO(oibfarhat): Embed statistics.
 */
public class SSDistStore {

    protected static final Logger LOG = LoggerFactory.getLogger(SSDistStore.class);

    private final long ssIndex;
    private boolean finalized;

    private double mean;
    private double sd;
    private long count;

    SSDistStore(long ssIndex) {
        this.ssIndex = ssIndex;
        this.mean = 0;
        this.sd = 0;
        this.count = 0;
    }


    void addValue(long networkDelay) {
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

//    /*
//     * Public interface for the user to add a specific delay.
//     */
//    public void add(long ssIndex, long delay) {
//        SSDelayProp subStreamProperties = runningSSMap.getOrDefault(ssIndex, null);
//
//        // New ss!!
//        if (subStreamProperties == null) {
//            subStreamProperties = new SSDelayProp(ssIndex);
//            runningSSMap.put(ssIndex, subStreamProperties);
//        }
//
//        subStreamProperties.addElement(delay);
//    }
//
//    /*
//     * Public interface for the user to signal an end of a ss
//     */
//    public void finalize(long ssIndex) {
//        SSDelayProp props = runningSSMap.remove(ssIndex);
//        if (props == null) {
//            LOG.info("Finalizing ss %d", ssIndex);
//            return;
//        }
//
//        props.finalizeSubstream();
//        historicalSSList.addLast(props);
//        while (historicalSSList.size() >= historySize) {
//            historicalSSList.removeFirst();
//        }
//        this.ssWatermark = Math.max(this.ssWatermark, ssIndex);
//    }
//
//    /*
//     * Summarize ss
//     */
//    public SSDelayProp estimate(long ssIndex) {
//        // Pull from history
//        if (this.ssWatermark > ssIndex) {
//            for (SSDelayProp subStreamProperties : historicalSSList)
//                if (subStreamProperties.ssIndex == ssIndex)
//                    return subStreamProperties;
//            // Element not found, too old
//            return null;
//        }
//
//        double mean = 0;
//        double sd = 0;
//        long count = 0;
//        for (SSDelayProp historicalSubStream : historicalSSList) {
//            mean += historicalSubStream.mean;
//            sd += historicalSubStream.sd;
//            count++;
//        }
//        return new SSDelayProp(ssIndex, mean / count, sd / count, count);
//    }
//
//
//}
//
