package org.apache.flink.streaming.api.operators.watslack.sampling;

class SamplingPlan {
    private final long windowIndex;
    private final int ssSize;

    private final long[] ssEventsNum;
    private final double[] samplingRates;
    private final boolean[] isFactual;


    SamplingPlan(
            final long windowIndex,
            final int ssSize) {
        this.windowIndex = windowIndex;
        this.ssSize = ssSize;

        this.ssEventsNum = new long[ssSize];
        this.samplingRates = new double[ssSize];
        this.isFactual = new boolean[ssSize];
    }

    double getSamplingRatio(int ssLocalIndex) {
        return samplingRates[ssLocalIndex];
    }

    long getTargetEvents(int ssLocalIndex) {
        return ssEventsNum[ssLocalIndex];
    }

    void updatePlanFacts(int ssIndex, long observedEvents, double samplingRate) {
        for (int i = 0; i < ssIndex; i++) {
            assert isFactual[i];
        }

        isFactual[ssIndex] = true;
        ssEventsNum[ssIndex] = observedEvents;
        samplingRates[ssIndex] = samplingRate;
    }

    void updatePlanEstimation(int ssIndex, long projectedEvents, double targetSamplingRate) {
        if (isFactual[ssIndex]) {
            // Cannot update these values!
            return;
        }

        this.ssEventsNum[ssIndex] = projectedEvents;
        this.samplingRates[ssIndex] = targetSamplingRate; //
    }
}
