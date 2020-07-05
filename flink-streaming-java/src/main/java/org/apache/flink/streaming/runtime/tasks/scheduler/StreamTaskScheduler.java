package org.apache.flink.streaming.runtime.tasks.scheduler;

import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.taskexecutor.scheduler.AbstractTaskScheduler;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.scheduler.algorithm.SchedulerAlgorithm;
import org.apache.flink.streaming.runtime.tasks.scheduler.algorithm.SchedulerAlgorithmsFactory;

import java.util.List;
import java.util.stream.Collectors;

public class StreamTaskScheduler extends AbstractTaskScheduler implements StreamTasksListener {


    private SchedulerAlgorithm schedulerAlgorithm;

    /* Boolean flag that identify changes in the Tasks list to reduce scheduling overhead. */
    private boolean streamTasksListChanged = false;

    @Override
    public void invoke() throws Exception {
        schedulerAlgorithm = SchedulerAlgorithmsFactory.createSchedulerAlgorithm(
                getSchedulerName(), this, this.getMetricGroup());

        LOG.info("Running {}", getSchedulerName());
        /* Invoke! */
        try {
            long start = System.currentTimeMillis();
            schedulerAlgorithm.invoke();
            long stop = System.currentTimeMillis();

            // TODO(oibfarhat): Use API to fetch these metrics.
            printStatistics(getScheduledTasks(), stop - start);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean addInvokable(AbstractInvokable invokable) {
        if (invokable == null) {
            return false;
        }

        LOG.info("Received {} of instance {}", ((StreamTask) invokable).getName(), invokable.getClass());
        boolean res = scheduledTasks.add(invokable);
        this.streamTasksListChanged = streamTasksListChanged || res;
        return res;
    }

    @Override
    public boolean removeInvokable(AbstractInvokable invokable) {
        if (invokable == null) {
            return false;
        }

        if (schedulerAlgorithm != null) {
            schedulerAlgorithm.cancel();
        }

        try {
            Thread.sleep(15000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public List<StreamTask> getScheduledTasks() {
        streamTasksListChanged = false;
        return scheduledTasks
                .stream()
                .map(abstractInvokable -> (StreamTask) abstractInvokable)
                .collect(Collectors.toList());
    }

    private void printStatistics(List<? extends AbstractInvokable> scheduledTasks, long timeInMS) {
        // NumOfRecords
        int numOfProcessedRecords = 0;
        for (int i = 0; i < scheduledTasks.size(); i++) {
            StreamTask task = (StreamTask) scheduledTasks.get(i);
            OperatorIOMetricGroup ioMetricGroup = ((OperatorMetricGroup)
                    (task.getHeadOperator().getMetricGroup()))
                    .getIOMetricGroup();

            numOfProcessedRecords += ioMetricGroup.getNumRecordsOutCounter().getCount();
        }

        long timeInSec = (timeInMS) / 1000;
        // Global variables
        System.out.println("Experiment ran for " + timeInMS + " ms");
        System.out.println("Number of records processed\t" + numOfProcessedRecords);
        System.out.println("Throughput\t" + numOfProcessedRecords / timeInSec); // length of experiment
        System.out.println();

        /* Scheduler specific variables */
        System.out.println("Scheduler: " + this.getSchedulerName());
        System.out.println("===============");
        schedulerAlgorithm.printStats();
        System.out.println();

    }


    @Override
    public boolean hasChanged() {
        return streamTasksListChanged;
    }
}
