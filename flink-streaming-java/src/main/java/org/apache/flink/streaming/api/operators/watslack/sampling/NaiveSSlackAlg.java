package org.apache.flink.streaming.api.operators.watslack.sampling;

public class NaiveSSlackAlg extends AbstractSSlackAlg {

    public NaiveSSlackAlg(long windowLength, long ssLength, int ssSize) {
        super(windowLength, ssLength, ssSize);

    }

    @Override
    public boolean sample(long event) {
        return true;
    }
    
}
