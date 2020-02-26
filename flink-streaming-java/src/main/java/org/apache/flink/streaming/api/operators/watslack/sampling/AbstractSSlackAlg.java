package org.apache.flink.streaming.api.operators.watslack.sampling;

import org.apache.flink.streaming.api.operators.watslack.WindowSSlackManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * This class determines sampling rates across for each substream.
 * <p>
 * It is possible to update the sampling values when real sampling rate values are collected.
 */

public abstract class AbstractSSlackAlg {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractSSlackAlg.class);

    private static final double MIN_SAMPLING_RATE = 0.1;

    protected final WindowSSlackManager windowSSlackManager;
    protected final long windowLength;
    protected final long ssLength;
    protected final int ssSize;
    /* Data Structures */
    protected final Map<Long, SamplingPlan> samplingPlanMap;
    /* Utils */
    protected final Random random;

    AbstractSSlackAlg(
            final WindowSSlackManager windowSSlackManager,
            final long windowLength, final long ssLength, final int ssSize) {
        this.windowSSlackManager = windowSSlackManager;
        this.windowLength = windowLength;
        this.ssLength = ssLength;
        this.ssSize = ssSize;

        this.samplingPlanMap = new HashMap<>();

        this.random = new Random();
    }

    public abstract void initiatePlan(long windowIndex);

    protected abstract void updatePlan(long windowIndex);

    public void updateAfterPurging(long windowIndex, int ssIndex, long observedEvents, double samplingRate) {
        SamplingPlan plan = samplingPlanMap.getOrDefault(windowIndex, null);
        if (plan == null) {
            LOG.warn("Sampling plan is null for window {}.{}.", windowIndex, ssIndex);
            return;
        }

        plan.updatePlanFacts(ssIndex, observedEvents, samplingRate);
        /* Estimate the newest values. */
        updatePlan(windowIndex);

    }

    public boolean sample(long windowIndex, int localSSIndex) {
        // Avoid sampling when warm-up is not done!
        if (!windowSSlackManager.isWarmedUp()) {
            return true;
        }
        // Watermark already emitted
        if (windowSSlackManager.getLastEmittedWatermark() >= windowSSlackManager.getSSDeadline(windowIndex, localSSIndex)) {
            return false;
        }
        return random.nextDouble() <= samplingPlanMap.get(windowIndex).getSamplingRatio(localSSIndex);
    }

    public long emitWatermark(long windowIndex, int localSSIndex, long observedEvents, double samplingRate) {
        // Avoid sampling when warm-up is not done!
        if (!windowSSlackManager.isWarmedUp()) {
            return -1;
        }
        long watermarkTarget = windowSSlackManager.getSSDeadline(windowIndex, localSSIndex);
        // Watermark already emitted
        if (windowSSlackManager.getLastEmittedWatermark() >= watermarkTarget) {
            return -1;
        }
        SamplingPlan plan = samplingPlanMap.getOrDefault(windowIndex, null);
        if (plan.getTargetEvents(localSSIndex) <= observedEvents && plan.getSamplingRatio(localSSIndex) <= samplingRate) {
            return watermarkTarget;
        }
        return -1;
    }
}
