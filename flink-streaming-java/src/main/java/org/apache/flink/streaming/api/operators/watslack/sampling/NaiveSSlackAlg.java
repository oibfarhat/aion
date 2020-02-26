package org.apache.flink.streaming.api.operators.watslack.sampling;

import org.apache.flink.streaming.api.operators.watslack.WindowSSlackManager;

public class NaiveSSlackAlg extends AbstractSSlackAlg {

    public NaiveSSlackAlg(
            final WindowSSlackManager sSlackManager,
            final long windowLength,
            final long ssLength,
            final int ssSize) {
        super(sSlackManager, windowLength, ssLength, ssSize);

    }

    @Override
    protected void initiatePlan(long windowIndex) {
        SamplingPlan samplingPlan = new SamplingPlan(windowIndex, ssSize);
        for (int i = 0; i < ssSize; i++) {
            samplingPlan.updatePlanFacts(i, 5, 0.2);
        }
        samplingPlanMap.put(windowIndex, samplingPlan);
    }

    @Override
    protected void updatePlan(long windowIndex) {
        // NO UPDATES
    }
}