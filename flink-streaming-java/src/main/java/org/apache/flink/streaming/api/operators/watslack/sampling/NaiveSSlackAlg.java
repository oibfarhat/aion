package org.apache.flink.streaming.api.operators.watslack.sampling;

import org.apache.flink.streaming.api.operators.watslack.WindowSSlack;
import org.apache.flink.streaming.api.operators.watslack.WindowSSlackManager;
import org.apache.flink.streaming.api.operators.watslack.estimators.WindowSizeEstimator;

public class NaiveSSlackAlg extends AbstractSSlackAlg {

    private static final double TARGET_SR = 0.7;

    public NaiveSSlackAlg(
            final WindowSSlackManager sSlackManager,
            final WindowSizeEstimator srEstimator) {
        super(sSlackManager, srEstimator);
    }

    @Override
    public void initiatePlan(WindowSSlack windowSSlack) {
        SamplingPlan samplingPlan =
                new SamplingPlan(windowSSlack, windowSSlackManager.getSSSize());
        samplingPlanMap.put(windowSSlack, samplingPlan);

        long ssEventsNum = srEstimator.getEventsNumPerSS(samplingPlan);
        for (int i = 0; i < windowSSlackManager.getSSSize(); i++) {
            samplingPlan.updatePlan(i, ssEventsNum, TARGET_SR);
        }
    }

    @Override
    protected void updatePlan(WindowSSlack windowSSlack) {
        SamplingPlan plan = samplingPlanMap.get(windowSSlack);
        long ssEventsNum = srEstimator.getEventsNumPerSS(plan);

        for (int i = 0; i < windowSSlackManager.getSSSize(); i++) {
            if (!plan.isSSPurged(i))
                plan.updatePlan(i, ssEventsNum, TARGET_SR);
        }
    }
}