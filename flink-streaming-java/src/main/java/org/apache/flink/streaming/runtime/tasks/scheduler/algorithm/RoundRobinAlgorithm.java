package org.apache.flink.streaming.runtime.tasks.scheduler.algorithm;

import org.apache.flink.runtime.metrics.groups.TaskSchedulerMetricGroup;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.scheduler.StreamTasksListener;
import org.apache.flink.streaming.runtime.tasks.scheduler.collectors.RecordsCountCollector;
import org.apache.flink.streaming.runtime.tasks.scheduler.SchedulerMonitor;

public class RoundRobinAlgorithm extends AbstractPriorityAlgorithm {

    RoundRobinAlgorithm(StreamTasksListener tasksListener, TaskSchedulerMetricGroup metricGroup, int numOfCores) {
        super(tasksListener, metricGroup, numOfCores);
    }

    @Override
    SchedulerMonitor createMonitor(StreamTask streamTask) {
        return new SchedulerMonitor(new RecordsCountCollector());
    }

    /*
     * The only metric considered in the RoundRobin algorithm is starvation.
     */
    @Override
    protected double computePriority(StreamTask task, long cycleNumber) {
        return cycleNumber - lastTaskCycle.get(task);
    }

    @Override
    protected long computeNeededTime(StreamTask task) {
        // zero is ignored
        return 0;
    }
}
