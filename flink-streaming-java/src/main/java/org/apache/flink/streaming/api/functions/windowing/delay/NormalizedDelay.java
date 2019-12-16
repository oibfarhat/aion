package org.apache.flink.streaming.api.functions.windowing.delay;

public class NormalizedDelay implements StreamDelayStats {

    long[] histDelay;
    long currDelayMean, currDelaySd;
    int lastEpoch;

    NormalizedDelay(int m) {
        histDelay = new long[m];
    }

    @Override
    public void collectDelay(long delay) {
        currDelayMean += delay;
    }
}
