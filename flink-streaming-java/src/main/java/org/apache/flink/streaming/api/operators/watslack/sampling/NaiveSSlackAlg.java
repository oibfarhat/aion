package org.apache.flink.streaming.api.operators.watslack.sampling;

public class NaiveSSlackAlg extends AbstractSSlackAlg {

    public NaiveSSlackAlg(long windowLength, long ssLength, int ssSize) {
        super(windowLength, ssLength, ssSize);

    }

    @Override
    public double getSamplingRates(int localSSIndex) {
        return 0.8;
    }
}
