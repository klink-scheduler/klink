package org.apache.flink.streaming.runtime.tasks.scheduler.algorithm;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.runtime.metrics.groups.TaskSchedulerMetricGroup;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.scheduler.StreamTasksListener;
import org.apache.flink.streaming.runtime.tasks.scheduler.collectors.NoCollector;
import org.apache.flink.streaming.runtime.tasks.scheduler.SchedulerMonitor;

/*
 * This scheduling policy is implemented only to measure the scheduling
 * overhead added by the bare architecture of the scheduler.
 * It provides a minimal implementation for comparison purposes.
 */

@Experimental
class OSSchedulerAlgorithm extends AbstractPriorityAlgorithm {

    OSSchedulerAlgorithm(StreamTasksListener tasksListener, TaskSchedulerMetricGroup metricGroup, int numOfCores) {
        super(tasksListener, metricGroup, Integer.MAX_VALUE);
    }

    @Override
    SchedulerMonitor createMonitor(StreamTask streamTask) {
        return new SchedulerMonitor(new NoCollector());
    }

    @Override
    protected double computePriority(StreamTask task, long cycleNumber) {
        return 0;
    }

    @Override
    protected long computeNeededTime(StreamTask task) {
        return 100;
    }
}
