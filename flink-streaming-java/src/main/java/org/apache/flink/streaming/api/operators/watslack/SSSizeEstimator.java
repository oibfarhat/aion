package org.apache.flink.streaming.api.operators.watslack;

import java.util.HashMap;
import java.util.Map;

public class SSSizeEstimator {

    private final NetworkDelayDist delayDist;
    private final InterEventGenDelayDist genDist;
    private final long watermarkFrequency;

    /* Caching */
    private Map<Long, Long> cachedSubstreams;

    public SSSizeEstimator(
            final NetworkDelayDist delayDist, final InterEventGenDelayDist genDist, final long watermarkFrequency) {
        this.delayDist = delayDist;
        this.genDist = genDist;
        this.watermarkFrequency = watermarkFrequency;

        this.cachedSubstreams = new HashMap<>();
    }

    // TODO(oibfarhat): Should we consider ranges?
    public long estimate(long substreamIndex) {
        if (cachedSubstreams.containsKey(substreamIndex)) {
            return cachedSubstreams.get(substreamIndex);
        }
        NetworkDelayDist.SSDelayProp delayProps = delayDist.estimate(substreamIndex);
        InterEventGenDelayDist.SSGenProp genProps = genDist.estimate(substreamIndex);
        long n = (long) Math.ceil((delayProps.mean + this.watermarkFrequency) * genProps.mean);

        cachedSubstreams.put(substreamIndex, n);
        return n;
    }

}
