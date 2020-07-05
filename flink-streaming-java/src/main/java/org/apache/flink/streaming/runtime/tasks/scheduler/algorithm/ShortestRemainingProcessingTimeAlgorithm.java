package org.apache.flink.streaming.runtime.tasks.scheduler.algorithm;

import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskSchedulerMetricGroup;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.scheduler.SchedulerMonitor;
import org.apache.flink.streaming.runtime.tasks.scheduler.StreamTasksListener;
import org.apache.flink.streaming.runtime.tasks.scheduler.collectors.RecordsCountCollector;

public class ShortestRemainingProcessingTimeAlgorithm extends AbstractPriorityAlgorithm {

    ShortestRemainingProcessingTimeAlgorithm(StreamTasksListener tasksListener, TaskSchedulerMetricGroup metricGroup, int numOfCores) {
        super(tasksListener, metricGroup, numOfCores, false);
    }

    @Override
    SchedulerMonitor createMonitor(StreamTask streamTask) {
        return new SchedulerMonitor(new RecordsCountCollector());
    }

    /*
     * The only metrics considered in the SRPT algorithm are queue size and processing time.
     */
    @Override
    protected double computePriority(StreamTask task, long cycleNumber) {
        // The lower the better!
        if (task.getHeadOperator() == null || task.getHeadOperator().getMetricGroup() == null) {
            return Integer.MAX_VALUE;
        }

        OperatorMetricGroup metrics = (OperatorMetricGroup) (task.getHeadOperator().getMetricGroup());
        double operatorCost =
                Math.max(metrics.getIOMetricGroup().getTotalProcessingTime().getStatistics().getMean(), 1);
        long queueSize = taskQueueSize.get(task);

        if (queueSize == 0) {
            // Do not schedule operators with empty queue size
            return Integer.MAX_VALUE;
        }

        return Math.ceil(operatorCost * queueSize);
    }

    @Override
    protected long computeNeededTime(StreamTask task) {
        return (long) Math.ceil(taskPriorities.get(task));
    }
}
