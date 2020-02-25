package org.apache.flink.streaming.api.operators.watslack.garbage;

public class SSSampleSizeEstimator {

    private final SSSizeEstimator sizeEstimator;

    public SSSampleSizeEstimator(
            final SSSizeEstimator sizeEstimator) {
        this.sizeEstimator = sizeEstimator;
    }

    // TODO(oibfarhat): Should we consider ranges?
    public long estimate(long substreamIndex) {
        return (long) Math.ceil(0.9 * sizeEstimator.estimate(substreamIndex));
    }

}
