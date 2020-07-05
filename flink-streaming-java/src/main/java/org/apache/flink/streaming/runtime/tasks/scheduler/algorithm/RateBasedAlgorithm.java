package org.apache.flink.streaming.runtime.tasks.scheduler.algorithm;

import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskSchedulerMetricGroup;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.scheduler.StreamTasksListener;
import org.apache.flink.streaming.runtime.tasks.scheduler.collectors.RecordsCountCollector;
import org.apache.flink.streaming.runtime.tasks.scheduler.SchedulerMonitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RateBasedAlgorithm extends AbstractPriorityAlgorithm {

    // Task to query identifier
    private final Map<StreamTask, Integer> taskToQuery;

    // Total number of queries.
    private int numOfQueries = 0;

    // Round of current query
    private int queryRound = 0;

    RateBasedAlgorithm(StreamTasksListener tasksListener, TaskSchedulerMetricGroup metricGroup, int numOfCores) {
        super(tasksListener, metricGroup, numOfCores);
        this.taskToQuery = new HashMap<>(4);
    }

    @Override
    SchedulerMonitor createMonitor(StreamTask streamTask) {
        return new SchedulerMonitor(new RecordsCountCollector());
    }

    /*
     * Associate each task to a query identifier.
     */
    @Override
    protected void setupChange(List<StreamTask> tasks, long cycleNumber) {
        taskToQuery.clear();

        int queryId = -1;
        for (StreamTask sinkTask : tasks) {
            if (!(sinkTask.getHeadOperator() instanceof StreamSink)) {
                continue;
            }

            queryId++;

            StreamTask currTask = sinkTask;
            while (currTask != null) {
                // Assign id to that operator
                taskToQuery.put(currTask, queryId);
                // Fetch previous operator
                currTask = getPredecessor(currTask);
            }
        }
        numOfQueries = queryId + 1;
    }

    @Override
    protected void setupCycle(List<StreamTask> tasks, long cycleNumber) {
        if (numOfQueries == 0) {
            this.queryRound = 0;
        } else {
            this.queryRound = (queryRound + 1) % numOfQueries;
        }
    }

    @Override
    protected void computePriorities(List<StreamTask> tasks, Map<StreamTask, Double> taskPriorities, long cycleNumber) {
        for (StreamTask sinkTask : tasks) {
            if (!(sinkTask.getHeadOperator() instanceof StreamSink)) {
                continue;
            }

            // Compute the priority inversely.
            StreamTask currTask = sinkTask;

            long chainProductivity = 1;
            long chainCost = 1;

            while (currTask != null && !(currTask instanceof SourceStreamTask)) {
                double priority;
                if (taskToQuery.get(currTask) != queryRound || currTask.getHeadOperator() == null
                        || currTask.getHeadOperator().getMetricGroup() == null) {
                    // Zero priority
                    priority = 0;
                } else {
                    // Compute the priority of that operator
                    long queueSize = taskQueueSize.get(currTask);

                    OperatorMetricGroup metrics = (OperatorMetricGroup) (currTask.getHeadOperator().getMetricGroup());
                    double operatorCost = metrics.getIOMetricGroup().getTotalProcessingTime().getStatistics().getMean();
                    double inRecords = metrics.getIOMetricGroup().getNumRecordsInCounter().getCount();
                    double outRecords = metrics.getIOMetricGroup().getNumRecordsOutCounter().getCount();
                    double selectivity = 1;

                    if (inRecords != 0 || outRecords != 0) {
                        selectivity = outRecords / inRecords;
                    }

                    // numerator
                    long productivity = 1 + (long) Math.ceil(selectivity * queueSize);
                    if (productivity != 0) {
                        chainProductivity *= productivity;
                    }
                    // denominator
                    if (operatorCost != 0) {
                        chainCost += operatorCost;
                    }
                    priority = chainProductivity / chainCost;
                    priority = 100;
                    if (productivity != 0) {
                        chainCost *= productivity;
                    }
                }

                // put priority
                taskPriorities.put(currTask, priority);

                // Fetch previous operator
                currTask = getPredecessor(currTask);
            }
        }
    }

    @Override
    protected double computePriority(StreamTask task, long cycleNumber) {
        // this function will not be called.
        throw new UnsupportedOperationException("This method should not have been called!");
    }

    /*
     * Total processing time times the input buffer size
     */
    @Override
    protected long computeNeededTime(StreamTask task) {
        return 25;
    }
}
