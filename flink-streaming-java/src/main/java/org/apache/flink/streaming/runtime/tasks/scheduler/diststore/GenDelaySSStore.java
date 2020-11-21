package org.apache.flink.streaming.runtime.tasks.scheduler.diststore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.PriorityQueue;

public class GenDelaySSStore implements SSDistStore {

    protected static final Logger LOG = LoggerFactory.getLogger(GenDelaySSStore.class);

    private final WindowDistStore windowDistStore;
    private final long ssIndex;

    private final PriorityQueue<Long> eventsQueue;
    private double mean;
    private double sd;
    private long count;
    private boolean isPurged;

    GenDelaySSStore(final WindowDistStore windowDistStore, final long ssIndex) {
        this.windowDistStore = windowDistStore;
        this.ssIndex = ssIndex;
        this.mean = 0;
        this.sd = 0;
        this.count = 0;
        this.isPurged = false;
        this.eventsQueue = new PriorityQueue<>();
    }

    @Override
    public long getWindowIndex() {
        return windowDistStore.getWindowIndex();
    }

    @Override
    public long getSSIndex() {
        return ssIndex;
    }

    @Override
    public boolean isPurged() {
        return isPurged;
    }

    @Override
    public void addValue(long eventTime) {
        if (isPurged) {
            LOG.warn("Attempting to add a value to a purged substream");
            return;
        }
        eventsQueue.add(eventTime);
    }

    @Override
    public void purge() {
        if (this.isPurged) {
            LOG.warn("Attempting to purge an already purged substream");
            return;
        }

        this.isPurged = true;
        if (eventsQueue.isEmpty()) {
            LOG.warn("Purging an empty substream");
            return;
        }

        long lastTs = eventsQueue.poll();
        while (!eventsQueue.isEmpty()) {
            long currTs = eventsQueue.poll();
            long genDelay = currTs - lastTs;
            mean += genDelay;
            sd += (genDelay * genDelay);
            count++;

            lastTs = currTs;
        }

        if (this.count > 0) {
            this.mean /= count;
            this.sd = (this.sd / this.count) - (this.mean * this.mean);
        }
        LOG.info("Purging SS {}.{}: (mu={}, sd={}, count={})", getWindowIndex(), ssIndex, mean, sd, count);
        this.eventsQueue.clear();
    }

    @Override
    public double getMean() {
        if (this.isPurged) {
            return this.mean;
        }
        LOG.warn("Retrieving the mean of a non-purged substream");
        return -1;
    }

    @Override
    public double getSD() {
        if (this.isPurged) {
            return this.sd;
        }
        LOG.warn("Retrieving the SD of a non-purged substream");
        return -1;
    }


    @Override
    public long getCount() {
        if (this.isPurged) {
            return this.count;
        }
        LOG.warn("Retrieving the count of a non-purged substream");
        return -1;
    }

    @Override
    public int compareTo(SSDistStore ssDistStore) {
        if (this.windowDistStore.getWindowIndex() != ssDistStore.getWindowIndex()) {
            return (int) (this.windowDistStore.getWindowIndex() - ssDistStore.getWindowIndex());
        } else {
            return (int) (this.getSSIndex() - ssDistStore.getSSIndex());
        }
    }
}