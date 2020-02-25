package org.apache.flink.streaming.api.operators.watslack.sampling;

/**
 * This class determines sampling rates across for each substream.
 *
 * It is possible to update the sampling values when real sampling rate values are collected.
 */

public abstract class SamplingSlackAlg {

    private static final double MIN_SAMPLING_RATE = 0.1;

    protected final long windowSize;
    protected final long ssSize;

    private final double[] samplingRates;

    public SamplingSlackAlg(
            final long windowSize, final long ssSize, final long numOfSS) {

        samplingRates = new long[numOfSS];
        samplingRates =
    }

    public long[] getSamplingRates();
}
