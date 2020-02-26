package org.apache.flink.streaming.api.operators.watslack.sampling;

import org.apache.flink.streaming.api.operators.watslack.WindowSSlack;
import org.apache.flink.streaming.api.operators.watslack.WindowSSlackManager;
import org.apache.flink.streaming.api.operators.watslack.estimators.WindowSizeEstimator;
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

    protected final WindowSSlackManager windowSSlackManager;
    protected final WindowSizeEstimator srEstimator;
    /* Data Structures */
    protected final Map<WindowSSlack, SamplingPlan> samplingPlanMap;
    /* Utils */
    protected final Random random;

    AbstractSSlackAlg(
            final WindowSSlackManager windowSSlackManager,
            final WindowSizeEstimator srEstimator) {
        this.windowSSlackManager = windowSSlackManager;
        this.srEstimator = srEstimator;

        this.samplingPlanMap = new HashMap<>();

        this.random = new Random();
    }

    public abstract void initiatePlan(WindowSSlack windowSSlack);

    protected abstract void updatePlan(WindowSSlack windowSSlack);

    public void updateAfterPurging(WindowSSlack windowSSlack, int localSSIndex) {
        SamplingPlan plan = samplingPlanMap.getOrDefault(windowSSlack, null);
        if (plan == null) {
            LOG.warn("Sampling plan is null for window {}.{}.", windowSSlack.getWindowIndex(), localSSIndex);
            return;
        }

        plan.updatePlanFacts(localSSIndex,
                windowSSlack.getObservedEvents(localSSIndex),
                windowSSlack.getSamplingRate(localSSIndex));
        /* Estimate the newest values. */
        updatePlan(windowSSlack);

    }

    public boolean sample(WindowSSlack windowSSlack, int localSSIndex) {
        // Avoid sampling when warm-up is not done!
        if (!windowSSlackManager.isWarmedUp()) {
            return true;
        }
        // Watermark already emitted
        if (windowSSlackManager.getLastEmittedWatermark() >=
                windowSSlackManager.getSSDeadline(windowSSlack.getWindowIndex(), localSSIndex)) {
            return false;
        }
        return random.nextDouble() <= samplingPlanMap.get(windowSSlack).getSamplingRatio(localSSIndex);
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
