package org.apache.flink.streaming.runtime.tasks.scheduler;

import org.apache.flink.streaming.runtime.tasks.scheduler.collectors.SchedulerCollector;

/**
 * A scheduler monitor is an object used for synchronization. A {@code StreamTask}
 * holding a monitor object would listen to any changes in the monitor status after
 * processing every element.
 */
public class SchedulerMonitor {

    private final SchedulerCollector schedulerCollector;

    /* Flag depicting the status of the task holding this monitor. */
    private boolean isScheduled;

    public SchedulerMonitor(SchedulerCollector schedulerCollector) {
        this.schedulerCollector = schedulerCollector;
    }

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    public void start() {
        this.schedulerCollector.reset();
        this.isScheduled = false;
    }

    public void pause() {
        this.isScheduled = false;
    }

    public void resume() {
        this.schedulerCollector.reset();
        this.isScheduled = true;
    }

    /* Getter for {@code isScheduled} */
    public boolean isScheduled() {
        return isScheduled;
    }

    // ------------------------------------------------------------------------
    //  Accessors
    // ------------------------------------------------------------------------

    /**
     * Provides access to the private scheduler collector
     *
     * @return The scheduler collector object.
     */
    public SchedulerCollector getSchedulerCollector() {
        return this.schedulerCollector;
    }
}
