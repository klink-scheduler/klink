package org.apache.flink.streaming.runtime.tasks.scheduler.diststore;

import org.apache.flink.streaming.runtime.tasks.scheduler.diststore.SSDistStore;
import org.apache.flink.streaming.runtime.tasks.scheduler.diststore.WindowDistStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetDelaySSStore implements SSDistStore {

    protected static final Logger LOG = LoggerFactory.getLogger(NetDelaySSStore.class);

    /* Identifiers */
    private final WindowDistStore windowDistStore;
    private final long ssIndex;

    private double mean;
    private double sd;
    private long count;
    private boolean isPurged;

    NetDelaySSStore(final WindowDistStore windowDistStore, final long ssIndex) {
        this.windowDistStore = windowDistStore;
        this.ssIndex = ssIndex;

        this.mean = 0;
        this.sd = 0;
        this.count = 0;
        this.isPurged = false;
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
    public void addValue(long netDelay) {
        if (isPurged) {
            LOG.warn("Attempting to add a value to a purged substream");
            return;
        }
        mean += netDelay;
        sd += (netDelay * netDelay);
        count++;
    }

    @Override
    public void purge() {
        if (this.isPurged) {
            LOG.warn("Attempting to purge an already purged substream");
            return;
        }

        if (this.count > 0) {
            this.mean /= count;
            this.sd = (this.sd / this.count) - (this.mean * this.mean);
        }
        LOG.info("Purging SS {}.{}: (mu={}, sd={}, count={})", getWindowIndex(), ssIndex, mean, sd, count);
        this.isPurged = true;
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