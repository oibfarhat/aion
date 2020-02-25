package org.apache.flink.streaming.api.operators.watslack.diststore;

/**
 * This interface is used to provide an API to maintain the recorded values.
 */
public interface SSDistStore {

    long getSSIndex();

    void addValue(long eventTime);

    void purge();
}
