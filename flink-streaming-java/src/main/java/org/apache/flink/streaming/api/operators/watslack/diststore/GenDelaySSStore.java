package org.apache.flink.streaming.api.operators.watslack.diststore;

import java.util.PriorityQueue;

public class GenDelaySSStore implements SSDistStore {

    private final long ssIndex;

    private final PriorityQueue<Long> eventsQueue;

    public GenDelaySSStore(final long ssIndex) {
        this.ssIndex = ssIndex;
        this.eventsQueue = new PriorityQueue<>();
    }

    @Override
    public long getSSIndex() {
        return ssIndex;
    }

    @Override
    public void addValue(long eventTime) {
        eventsQueue.add(eventTime);
    }

//    /*
//     * Public interface for the user to signal an end of a substream
//     */
//    public void finalize(long ssIndex) {
//        PriorityQueue<Long> pQueue = runningSSMap.remove(ssIndex);
//        if (pQueue == null)
//            return;
//
//        InterEventGenDelayDist.SSGenProp props;
//        if (pQueue.isEmpty()) {
//            props = new InterEventGenDelayDist.SSGenProp(ssIndex, 0, 0, 0);
//        } else {
//            double runningMean = 0;
//            double runningSD = 0;
//            long size = 0;
//
//            long lastTS = pQueue.poll();
//
//            while (!pQueue.isEmpty()) {
//                long currTS = pQueue.poll();
//                long genDelay = currTS - lastTS;
//                runningMean += genDelay;
//                runningSD += (genDelay * genDelay);
//                size++;
//
//                lastTS = currTS;
//            }
//
//            props = new InterEventGenDelayDist.SSGenProp(ssIndex, runningMean, runningSD, size);
//        }
//        // Add to history
//        historicalSSList.addLast(props);
//        while (historicalSSList.size() >= historySize) {
//            historicalSSList.removeFirst();
//        }
//        // Remove from Map
//        pQueue.clear();
//
//        this.substreamWatermark = Math.max(this.substreamWatermark, ssIndex);
//    }
//
//    /*
//     * Summarize substream
//     */
//    public InterEventGenDelayDist.SSGenProp estimate(long ssIndex) {
//        // Pull from history
//        if (this.substreamWatermark > ssIndex) {
//            for (InterEventGenDelayDist.SSGenProp subStreamProperties : historicalSSList)
//                if (subStreamProperties.ssIndex == ssIndex)
//                    return subStreamProperties;
//            // Element not found, too old
//            return null;
//        }
//
//        double mean = 0;
//        double sd = 0;
//        long count = 0;
//        for (InterEventGenDelayDist.SSGenProp historicalSubStream : historicalSSList) {
//            mean += historicalSubStream.mean;
//            sd += historicalSubStream.sd;
//            count++;
//        }
//        return new InterEventGenDelayDist.SSGenProp(ssIndex, mean / count, sd / count, count);
//    }

}
