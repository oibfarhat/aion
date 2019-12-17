package org.apache.flink.streaming.api.functions.windowing.delay;

import java.util.ArrayList;
import java.util.List;

class EpochDelay {

    final long epochStart, epochEnd;

    private List<Long> delayTimestamps;

    private long delayCount;
    private double delayMean, delaySd;

    // TODO(oibfarhat): Consider using it later.
    private boolean isComplete;

    EpochDelay(final long epochStart, final long epochEnd) {
        this.epochStart = epochStart;
        this.epochEnd = epochEnd;

        this.delayTimestamps = new ArrayList<>();

        this.delayCount = 0;
        this.delayMean = 0;
        this.delaySd = 0;

        this.isComplete = false;
    }

    /*
     * Collect delay
     */
    void collectDelay(long delay) {
        if (isComplete) {
            return;
        }

        this.delayTimestamps.add(delay);
        this.delayCount++;
        this.delayMean += delay;
    }

    /*
     * Signal the completion of this epoch.
     */
    void signalCompletion() {
        this.isComplete = true;

        this.delayMean /= delayCount;

        for (long delay : delayTimestamps) {
            this.delaySd += Math.pow(delay - delayMean, 2);
        }

        this.delaySd = Math.sqrt(this.delaySd);

        // Clear memory
        this.delayTimestamps.clear();
        this.delayTimestamps = null;
    }

    /*
     * Return the mean delay of this epoch.
     */
    double getMeanDelay() {
        return this.delayMean;
    }

    /*
     * Return the standard deviation delay of this epoch.
     */
    double getSDDelay() {
        return this.delaySd;
    }

}
