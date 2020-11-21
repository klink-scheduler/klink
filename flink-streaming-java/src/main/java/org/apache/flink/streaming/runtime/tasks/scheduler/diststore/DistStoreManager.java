package org.apache.flink.streaming.runtime.tasks.scheduler.diststore;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;

import java.util.*;

public class DistStoreManager {

    public enum DistStoreType {
        NET_DELAY,
        GEN_DELAY
    }

    private final DistStoreType storeType;
    private final Map<WindowOperator, WindowDistStore> distStoreByWindow;

    private final Set<SSDistStore> purgedSSSet;
    /* Metrics */
    private final Histogram meanDelayPerSS;

    public DistStoreManager(
            final DistStoreType storeType) {
        this.storeType = storeType;

        this.distStoreByWindow = new HashMap<>();
        this.purgedSSSet = new TreeSet<>();

        this.meanDelayPerSS = new DescriptiveStatisticsHistogram(2048);
    }

    public WindowDistStore createWindowDistStore(WindowOperator op) {
        WindowDistStore windowStore = new WindowDistStore(this, 2048);
        distStoreByWindow.put(op, windowStore);
        return windowStore;
    }

    /*
     * @return the set of purged substreams
     */
    public Set<SSDistStore> getPurgedData() {
        return purgedSSSet;
    }

    public boolean removeWindowDistStore(WindowOperator op) {
        WindowDistStore store = distStoreByWindow.remove(op);
        if (store == null) {
            return false;
        }
        purgedSSSet.removeIf(ssStore -> ssStore.getWindowIndex() == Integer.valueOf(op));
        return true;
    }

    public void addPurgedSS(SSDistStore purgedSS) {
        purgedSSSet.add(purgedSS);

        meanDelayPerSS.update((long) Math.ceil(purgedSS.getMean()));
    }

    public HistogramStatistics getMeanDelay() {
        return meanDelayPerSS.getStatistics();
    }

    DistStoreType getStoreType() {
        return storeType;
    }

}