package org.apache.flink.streaming.api.operators.watslack.diststore;

public class WindowDistStore {

    private final long windowIndex;

    private SSDistStore[] ssStores;

    public WindowDistStore(
            final long windowIndex,
            final int ssSize) {
        this.windowIndex = windowIndex;

        ssStores = new SSDistStore[ssSize];
    }

    public void addEvent(int ssLocalIndex, long value) {
        ssStores[ssLocalIndex].addValue(value);
    }
}
