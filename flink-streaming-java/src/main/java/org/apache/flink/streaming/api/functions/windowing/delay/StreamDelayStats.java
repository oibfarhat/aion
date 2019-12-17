package org.apache.flink.streaming.api.functions.windowing.delay;

/**
 * Source delay delay based on .
 */
public interface StreamDelayStats {

    void collectDelay(long timestamp, long delay);
}
