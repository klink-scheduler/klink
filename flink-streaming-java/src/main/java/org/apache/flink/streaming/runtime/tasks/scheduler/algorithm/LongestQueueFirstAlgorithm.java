package org.apache.flink.streaming.runtime.tasks.scheduler.algorithm;

import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskSchedulerMetricGroup;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.scheduler.StreamTasksListener;
import org.apache.flink.streaming.runtime.tasks.scheduler.collectors.RecordsCountCollector;
import org.apache.flink.streaming.runtime.tasks.scheduler.SchedulerMonitor;

public class LongestQueueFirstAlgorithm extends AbstractPriorityAlgorithm {

    LongestQueueFirstAlgorithm(StreamTasksListener tasksListener, TaskSchedulerMetricGroup metricGroup, int numOfCores) {
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
        return taskQueueSize.get(task);
    }

    /*
     * Total processing time times the input buffer size
     */
    @Override
    protected long computeNeededTime(StreamTask task) {
        if (task.getHeadOperator() == null || task.getHeadOperator().getMetricGroup() == null) {
            return 0;
        }

        OperatorMetricGroup metrics = (OperatorMetricGroup) (task.getHeadOperator().getMetricGroup());
        double operatorCost = metrics.getIOMetricGroup().getTotalProcessingTime().getStatistics().getMean();
        long queueSize = taskQueueSize.get(task);
        return (long) Math.ceil(operatorCost * queueSize);
    }
}
