package org.apache.flink.streaming.api.operators.watslack.sampling;

/**
 * This class determines sampling rates across for each substream.
 * <p>
 * It is possible to update the sampling values when real sampling rate values are collected.
 */

public abstract class AbstractSSlackAlg {

    private static final double MIN_SAMPLING_RATE = 0.1;

    protected final long windowLength;
    protected final long ssLength;
    protected final int ssSize;

    AbstractSSlackAlg(
            final long windowLength, final long ssLength, final int ssSize) {
        this.windowLength = windowLength;
        this.ssLength = ssLength;
        this.ssSize = ssSize;
    }

    public abstract boolean sample(long event);
}
