package org.apache.flink.streaming.runtime.tasks.scheduler.algorithm;

import org.apache.flink.runtime.metrics.groups.TaskSchedulerMetricGroup;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.streaming.runtime.tasks.scheduler.StreamTaskSchedulerPolicy;
import org.apache.flink.streaming.runtime.tasks.scheduler.StreamTasksListener;

public class SchedulerAlgorithmsFactory {

    /**
     * Creates and returns {@link SchedulerAlgorithm} using the scheduling policy.
     *
     * @param schedulerPolicy the key identifier of the scheduling algorithm
     * @param tasksListener   listener for stream tasks related events (add or cancel)
     * @return the scheduler algorithm
     */
    static SchedulerAlgorithm createSchedulerAlgorithm(StreamTaskSchedulerPolicy schedulerPolicy,
                                                       StreamTasksListener tasksListener, TaskSchedulerMetricGroup metricGroup) {
        int numOfCores = Hardware.getNumberCPUCores();

        if (schedulerPolicy == StreamTaskSchedulerPolicy.OS_SCHEDULER) {
            return new OSSchedulerAlgorithm(tasksListener, metricGroup, numOfCores);
        } else if (schedulerPolicy == StreamTaskSchedulerPolicy.ROUND_ROBIN) {
    //        return new RoundRobinAlgorithm(tasksListener, metricGroup, numOfCores);
        } else if (schedulerPolicy == StreamTaskSchedulerPolicy.LONGEST_QUEUE_FIRST) {
    //        return new LongestQueueFirstAlgorithm(tasksListener, metricGroup, numOfCores);
        } else if (schedulerPolicy == StreamTaskSchedulerPolicy.RATE_BASED) {
     //       return new RateBasedAlgorithm(tasksListener, metricGroup, numOfCores);
        } else if (schedulerPolicy == StreamTaskSchedulerPolicy.SHORTEST_REMAINING_PROCESSING_TIME) {
     //       return new ShortestRemainingProcessingTimeAlgorithm(tasksListener, metricGroup, numOfCores);
        } else if (schedulerPolicy == StreamTaskSchedulerPolicy.KLINK) {
            return new Klink(tasksListener, metricGroup, numOfCores);
        } else if (schedulerPolicy == StreamTaskSchedulerPolicy.FIRST_COME_FIRST_SERVED) {
     //       return new FirstComeFirstServeAlgorithm(tasksListener, metricGroup, numOfCores);
        }
        return null;
    }

    /**
     * Creates and returns {@link SchedulerAlgorithm} using the index of the scheduling policy.
     *
     * @param schedulerPolicy the key identifier of the scheduling algorithm
     * @param tasksListener   listener for stream tasks related events (add or cancel)
     * @return the scheduler algorithm
     */
    public static SchedulerAlgorithm createSchedulerAlgorithm(
            String schedulerPolicy, StreamTasksListener tasksListener, TaskSchedulerMetricGroup metricGroup) {
        return createSchedulerAlgorithm(StreamTaskSchedulerPolicy.fromName(schedulerPolicy), tasksListener, metricGroup);
    }
}
