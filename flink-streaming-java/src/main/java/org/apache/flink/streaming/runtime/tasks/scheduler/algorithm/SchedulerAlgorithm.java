package org.apache.flink.streaming.runtime.tasks.scheduler.algorithm;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskSchedulerMetricGroup;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.scheduler.StreamTasksListener;
import org.apache.flink.streaming.runtime.tasks.scheduler.SchedulerMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

// TODO(oibfarhat): Write documentation
public abstract class SchedulerAlgorithm {

    protected final Logger LOG = LoggerFactory.getLogger(SchedulerAlgorithm.class);
    /* Metric group */
    protected final MetricGroup metricGroup;
    /* Number of cores on the machine */
    protected final int numOfCores;
    /* Listener for stream tasks event */
    private final StreamTasksListener streamTasksListener;
    /* List of tasks */
    private List<StreamTask> tasksList;
    /* List of scheduled tasks */
    private List<StreamTask> scheduledTasksList;
    /* Boolean flag that depicts the status of the scheduler */
    private volatile boolean canceled = false;

    SchedulerAlgorithm(StreamTasksListener tasksListener, TaskSchedulerMetricGroup metricGroup, int numOfCores) {
        this.streamTasksListener = tasksListener;
        this.numOfCores = numOfCores;
        this.metricGroup = metricGroup.addGroup("taskscheduler");

        this.tasksList = new ArrayList<>(4);
        this.scheduledTasksList = new ArrayList<>();
    }

    /*
     * Registers a SchedulerMonitor.
     */
    private void registerMonitor(StreamTask streamTask, SchedulerMonitor schedulerMonitor) {
        SchedulerMonitor monitor = streamTask.getSchedulerMonitor();
        if (monitor != null) {
            return;
        }
        streamTask.setSchedulerMonitor(schedulerMonitor);
        // This does not start the task
        schedulerMonitor.start();
    }

    abstract SchedulerMonitor createMonitor(StreamTask streamTask);

    /*
     * Listens for changes from the scheduler.
     * This method is meant to be invoked regularly. It is not a clean implementation
     * so we should ultimately clean this later. However, currently it is not a top priority.
     */
    protected final boolean tasksListChanged() {
        if (streamTasksListener.hasChanged()) {
            /* Fetch the list of tasks */
            tasksList = streamTasksListener.getScheduledTasks();
            /* Fetch the list of schedulable tasks */
            scheduledTasksList = tasksList
                    .stream()
                    .filter(task -> !(task instanceof SourceStreamTask))
                    .collect(Collectors.toList());
            /* Register monitors. */
            scheduledTasksList.stream()
                    .filter(task -> task.getSchedulerMonitor() == null)
                    .forEach(task ->
                            registerMonitor(task, createMonitor(task)));
            return true;
        }
        return false;
    }

    /*
     * Algorithm logic goes here.
     */
    public abstract void invoke();

    /*
     * Cancel the execution.
     */
    public void cancel() {
        canceled = true;
    }

    protected boolean isCanceled() {
        return canceled;
    }

    /*
     * This method prints the collected statistics.
     */
    public abstract void printStats();

    protected final List<StreamTask> getTasksList() {
        return tasksList;
    }

    protected final List<StreamTask> getScheduledTasksList() {
        return scheduledTasksList;
    }
}
