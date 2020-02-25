package org.apache.flink.streaming.api.operators.watslack.diststore;

public class WindowDistStore<T extends SSDistStore> {

    private final long windowIndex;

    private T[] ssStores;

    public WindowDistStore(
            final long windowIndex,
            final int ssSize) {
        this.windowIndex = windowIndex;

        ssStores = (T[]) new SSDistStore[ssSize];
    }

    public void addEvent(int ssLocalIndex, long value) {
        ssStores[ssLocalIndex].addValue(value);
    }
}
