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

    public void initWindow(WindowSSlack windowSSlack) {
        SamplingPlan samplingPlan =
                new SamplingPlan(windowSSlack, windowSSlackManager.getSSSize());
        samplingPlanMap.put(windowSSlack, samplingPlan);

        if (windowSSlackManager.isWarmedUp()) {
            initiatePlan(windowSSlack, samplingPlan);
        }
    }

    public void updateAfterPurging(WindowSSlack windowSSlack, int localSSIndex) {
        SamplingPlan plan = samplingPlanMap.getOrDefault(windowSSlack, null);
        if (plan == null) {
            LOG.warn("Sampling plan is null for window {}.{}.", windowSSlack.getWindowIndex(), localSSIndex);
            return;
        }

        /* Purge the SS part of the plan. */
        plan.purgeSS(localSSIndex);
        if (!windowSSlackManager.isWarmedUp()) {
            return;
        }
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
            if (windowSSlack.isPurged(localSSIndex)) {
                LOG.warn("Attempting to add data to a purged window.");
            }
            return false;
        }
        return random.nextDouble() <= samplingPlanMap.get(windowSSlack).getSamplingRate(localSSIndex);
    }

    public long emitWatermark(WindowSSlack windowSSlack, int localSSIndex, long observedEvents) {
        // Avoid sampling when warm-up is not done!
        if (!windowSSlackManager.isWarmedUp()) {
            return -1;
        }
        long watermarkTarget = windowSSlackManager.getSSDeadline(windowSSlack.getWindowIndex(), localSSIndex);
        // Watermark already emitted
        if (windowSSlackManager.getLastEmittedWatermark() >= watermarkTarget) {
            return -1;
        }
        SamplingPlan plan = samplingPlanMap.getOrDefault(windowSSlack, null);
        if (plan.getObservedEvents(localSSIndex) <= observedEvents) {
            return watermarkTarget;
        }
        return -1;
    }

    protected abstract void initiatePlan(WindowSSlack windowSSlack, SamplingPlan samplingPlan);

    protected abstract void updatePlan(WindowSSlack windowSSlack);

}
