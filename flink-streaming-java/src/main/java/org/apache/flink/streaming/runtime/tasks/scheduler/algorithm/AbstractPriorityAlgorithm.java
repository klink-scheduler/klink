package org.apache.flink.streaming.runtime.tasks.scheduler.algorithm;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskSchedulerMetricGroup;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.scheduler.SchedulerMonitor;
import org.apache.flink.streaming.runtime.tasks.scheduler.StreamTasksListener;
import org.apache.flink.streaming.util.LatencyStats;

import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractPriorityAlgorithm extends SchedulerAlgorithm {

    protected static final int MIN_TASK_TIMEOUT = 25; // in ms
    protected static final int MAX_TASK_TIMEOUT = 200; // in ms
    private static final int HISTOGRAM_SIZE = 2048;

    /* Data structures updated per set of tasks. */
    private final Map<StreamTask, List<StreamTask>> taskSuccessors;
    private final Map<StreamTask, List<StreamTask>> taskPredecessors;

    /* Data structures updated per cycle. */
    protected final Map<StreamTask, Long> lastTaskCycle;
    protected final Map<StreamTask, Long> taskQueueSize;
    protected final Map<StreamTask, Double> taskPriorities;
    /* Metrics */
    // scheduler specific
    private final Counter numOfCyclesCount;
    private final Histogram schedulerOverheadHisto;
    private final Histogram sleepDurationHisto;
    private final Histogram numberOfRunningTasksHisto;
    // number of events
    private final Histogram numOfEventsOfAllTasksHisto;
    private final Histogram numOfEventsOfScheduledTasksHisto;
    private final Histogram numOfEventsProcessedHisto;
    // starvation
    private final Histogram starvationHisto;
    private boolean highestPriorityFirst = true;

    AbstractPriorityAlgorithm(StreamTasksListener tasksListener, TaskSchedulerMetricGroup metricGroup, int numOfCores) {
        super(tasksListener, metricGroup, numOfCores);

        // Data structures updated per tasks.
        this.taskSuccessors = new HashMap<>(4);
        this.taskPredecessors = new HashMap<>(4);

        // Data structures updated per cycle
        this.lastTaskCycle = new HashMap<>(4);
        this.taskQueueSize = new HashMap<>(4);
        this.taskPriorities = new HashMap<>(4);

        // metrics
        this.numOfCyclesCount = metricGroup.counter("numOfCyclesCount");
        this.schedulerOverheadHisto = metricGroup.histogram("schedulerOverhead", new DescriptiveStatisticsHistogram(HISTOGRAM_SIZE));
        this.sleepDurationHisto = metricGroup.histogram("sleepDuration", new DescriptiveStatisticsHistogram(HISTOGRAM_SIZE));
        this.numberOfRunningTasksHisto = metricGroup.histogram("numberOfRunningTasks", new DescriptiveStatisticsHistogram(HISTOGRAM_SIZE));

        this.numOfEventsOfAllTasksHisto = metricGroup.histogram("numOfEventsOfAllTasksHisto", new DescriptiveStatisticsHistogram(HISTOGRAM_SIZE));
        this.numOfEventsOfScheduledTasksHisto = metricGroup.histogram("numOfEventsOfScheduledTasksHisto", new DescriptiveStatisticsHistogram(HISTOGRAM_SIZE));
        this.numOfEventsProcessedHisto = metricGroup.histogram("numOfEventsProcessedHisto", new DescriptiveStatisticsHistogram(HISTOGRAM_SIZE));

        this.starvationHisto = metricGroup.histogram("starvationHisto", new DescriptiveStatisticsHistogram(HISTOGRAM_SIZE));
    }

    // ------------------------------------------------------------------------
    //  core methods
    // ------------------------------------------------------------------------

    AbstractPriorityAlgorithm(StreamTasksListener tasksListener, TaskSchedulerMetricGroup metricGroup, int numOfCores, boolean highestPriorityFirst) {
        this(tasksListener, metricGroup, numOfCores);
        this.highestPriorityFirst = highestPriorityFirst;
    }

    /**
     * The abstract algorithm is divided into five main stages:
     * <p>
     * 1) The first stage computes the priority of each task based on on the specified metric
     * for this algorithm.
     * <p>
     * 2) The second stage determines the top {@param numOfCores} tasks to schedule for the
     * current cycle.
     * <p>
     * 3) The third stage shuts down the tasks running from the old cycle. This stage is here as
     * an optimization to allow for the scheduler to run concurrently with operator processing.
     * It also helps in avoiding synchronization performance penalties.
     * <p>
     * 4) The fourth stage schedules the determined tasks in stage 2.
     * <p>
     * 5) The fifth stage shuts down the scheduler for a time period determined by the algorithm.
     */
    @Override
    public final void invoke() {
        /* Wait until tasks arrive. */
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        List<StreamTask> scheduledTasks = new ArrayList<>();

        List<StreamTask> prevRunningTasks = new ArrayList<>();
        List<StreamTask> currRunningTasks = new ArrayList<>();

        PriorityQueue<StreamTask> pq = new PriorityQueue<>(taskWithPrioritiesComparator);

        long cycleNumber;
        long startOverhead;

        while (!isCanceled()) {
            /* Stage 0: Cycle configuration */
            numOfCyclesCount.inc();
            cycleNumber = numOfCyclesCount.getCount();
            startOverhead = System.currentTimeMillis();

            if (tasksListChanged()) {
                updateTaskDependencies(getTasksList());
                // Place new tasks in lastTaskCycle
                scheduledTasks = getScheduledTasksList();
                scheduledTasks.stream()
                        .filter(task -> lastTaskCycle.get(task) == null)
                        .forEach(task -> lastTaskCycle.put(task, (long) 0));
                setupChange(getTasksList(), cycleNumber);
            }

            setupCycle(scheduledTasks, cycleNumber);
            // bookkeeping
            long eventsOfAllTasks = 0;
            long totalStarvation = 0;
            for (StreamTask task : scheduledTasks) {
                eventsOfAllTasks += updateQueueSize(task);
                totalStarvation += cycleNumber - lastTaskCycle.get(task);
            }
            numOfEventsOfAllTasksHisto.update(eventsOfAllTasks);
            starvationHisto.update(totalStarvation);

            /* Stage 1: Compute task priorities. */
            pq.clear();
            computePriorities(scheduledTasks, taskPriorities, cycleNumber);
            pq.addAll(scheduledTasks);

            /* Stage 2: Determine current cycle tasks. */
            currRunningTasks.clear();

            while (pq.size() > 0 && numOfCores > currRunningTasks.size()) {
                scheduleRunningTask(pq.poll(), currRunningTasks, cycleNumber);
            }

            // bookkeeping
            long eventsOfScheduledTasks = 0;
            long minProcessingTime = MAX_TASK_TIMEOUT;

            for (StreamTask task : currRunningTasks) {
                lastTaskCycle.put(task, cycleNumber);
                // metrics
                eventsOfScheduledTasks += taskQueueSize.get(task);
                minProcessingTime = Math.min(minProcessingTime, computeNeededTime(task));
            }
            minProcessingTime = Math.max(minProcessingTime, MIN_TASK_TIMEOUT);
            numOfEventsOfScheduledTasksHisto.update(eventsOfScheduledTasks);
            numberOfRunningTasksHisto.update(currRunningTasks.size());

            /* Stage 3: Shuts down previous cycle tasks. */
            long eventsProcessed = 0;
            // Cancel tasks running from previous cycle
            for (StreamTask prevTask : prevRunningTasks) {
                SchedulerMonitor monitor = prevTask.getSchedulerMonitor();
                eventsProcessed += monitor.getSchedulerCollector().numOfCollectedRecords();
                if (currRunningTasks.contains(prevTask)) {
                    // Avoid aborts for tasks that are about to be scheduled in this cycle
                    continue;
                }
                shutDownMonitor(monitor);
            }
            numOfEventsProcessedHisto.update(eventsProcessed);

            /* Stage 4: Schedule current cycle tasks. */
            for (StreamTask currTask : currRunningTasks) {
                SchedulerMonitor monitor = currTask.getSchedulerMonitor();

                activateMonitor(monitor);

                /*
                 * Wake up the thread if its not running.
                 * This optimization turns off the synchronization required.
                 */
                if (!prevRunningTasks.contains(currTask)) {
                    synchronized (monitor) {
                        monitor.notify();
                    }
                }
            }

            // Avoid garbage collection
            prevRunningTasks.clear();
            prevRunningTasks.addAll(currRunningTasks);

            schedulerOverheadHisto.update(System.currentTimeMillis() - startOverhead);
            /* Stage 5: sleep until time-out. */
            try {
                sleepDurationHisto.update(minProcessingTime);
                Thread.sleep(minProcessingTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Do some special setup on tasks list change.
     * <p>
     * The default implementation assumes no special setup required.
     * so no need to implement anything on top.
     *
     * @param tasks       the list of task that we are setting up
     * @param cycleNumber the cycle number
     */
    protected void setupChange(List<StreamTask> tasks, long cycleNumber) {
    }

    /**
     * Do some special setup on every new cycle
     * <p>
     * The default implementation assumes no special setup required.
     * so no need to implement anything on top.
     *
     * @param tasks       the list of task that we are setting up
     * @param cycleNumber the cycle number
     */
    protected void setupCycle(List<StreamTask> tasks, long cycleNumber) {
    }


    /**
     * Computes the priorities collectively based on all scheduled tasks and
     * places them back in a map.
     * <p>
     * This method is invoked in stage one.
     * <p>
     * The priority can be a function of multiple metrics including e.g.:
     * - Buffer size.
     * - Operator position in the pipeline.
     * - Operator selectivity and cost.
     * - Watermarks.
     * - etc.
     * <p>
     * The default implementation assumes independent metrics
     * so no need to implement anything on top.
     *
     * @param tasks       the list of task that we are calculating the priority of.
     * @param cycleNumber the cycle number
     */
    protected void computePriorities(List<StreamTask> tasks, Map<StreamTask, Double> taskPriorities, long cycleNumber) {
        for (StreamTask task : tasks) {
            taskPriorities.put(task, computePriority(task, cycleNumber));
        }
    }

    /**
     * Schedules the task with appropriate logic.
     * <p>
     * This method is invoked in stage two. The default behavior would be to just schedule that task.
     * <p>
     * The priority can be a function of multiple metrics including e.g.:
     * - Buffer size.
     * - Operator position in the pipeline.
     * - Operator selectivity and cost.
     * - Watermarks.
     * - etc.
     * <p>
     * The default implementation assumes independent metrics
     * so no need to implement anything on top.
     *
     * @param task         the task that we are scheduling with its priority
     * @param tasksRunning the list of tasks that were added to the running list
     * @param cycleNumber  the cycle number
     */
    protected void scheduleRunningTask(StreamTask task,
                                       List<StreamTask> tasksRunning, long cycleNumber) {
        tasksRunning.add(task);
    }

    /**
     * Computes the priority of each task. This method is invoked in stage one.
     * <p>
     * The priority can be a function of multiple metrics including e.g.:
     * - Buffer size.
     * - Operator position in the pipeline.
     * - Operator selectivity and cost.
     * - Watermarks.
     * - etc.
     *
     * @param task        the task that we are calculating the priority of.
     * @param cycleNumber the cycle number
     * @return the priority of that task.
     */
    protected abstract double computePriority(StreamTask task, long cycleNumber);

    /**
     * Computes the processing time needed for this scheduling round.
     * This method is invoked at the end of stage two.
     * <p>
     * The computation can be a function of multiple metrics including e.g.:
     * - Load.
     * - Watermark arrival.
     * - input buffer
     *
     * @param task the task that we are calculating the priority of.
     * @return the priority of that task.
     */
    protected abstract long computeNeededTime(StreamTask task);

    /*
     * Activates the passed monitor.
     * This function implementation is specific to the monitor being used in the
     * algorithm.
     *
     * {@param schedulerMonitor} the monitor to activate.
     */
    protected final void activateMonitor(SchedulerMonitor schedulerMonitor) {
        schedulerMonitor.resume();
    }

    // ------------------------------------------------------------------------
    //  bookkeeping methods
    // ------------------------------------------------------------------------

    /*
     * Shuts down the passed monitor.
     * This function implementation is specific to the monitor being used in the
     * algorithm.
     *
     * {@param schedulerMonitor} the monitor to shut down.
     */
    protected final void shutDownMonitor(SchedulerMonitor schedulerMonitor) {
        schedulerMonitor.pause();
    }

    /*
     * Updates taskSuccessors and taskPredecessors map.
     * It iterates over the graph of tasks and updates the data structures.
     *
     * {@param tasks} list of running tasks.
     */
    private void updateTaskDependencies(List<StreamTask> tasks) {
        taskSuccessors.clear();
        taskPredecessors.clear();

        // Map operator ids to tasks for faster access.
        Map<Integer, StreamTask> operatorIdToTask = new HashMap<>(4);
        Map<StreamTask, List<Integer>> taskToOutputOperatorsIds = new HashMap<>(4);

        tasks.forEach(task -> {
            List<Integer> outputIds = new ArrayList<>();
            // take care of chained operators
            task.getConfiguration().getTransitiveChainedTaskConfigsWithSelf(task.getUserCodeClassLoader())
                    .values().forEach(taskConfig -> {
                // Map operatorId to containing task
                operatorIdToTask.put(taskConfig.getVertexID(), task);
                // Map task to all output operator ids
                outputIds.addAll(taskConfig.getNonChainedOutputs(task.getUserCodeClassLoader())
                        .stream().map(StreamEdge::getTargetId).collect(Collectors.toList()));
            });
            taskToOutputOperatorsIds.put(task, outputIds);
        });

        // Build predecessors and successors
        for (Map.Entry<StreamTask, List<Integer>> taskToOutputOperator : taskToOutputOperatorsIds.entrySet()) {
            StreamTask currTask = taskToOutputOperator.getKey();
            taskToOutputOperator.getValue().stream().map(operatorIdToTask::get).forEach(subsqTask -> {
                // subsqTask predecessor is currTask
                List<StreamTask> predList = taskPredecessors.getOrDefault(subsqTask, new ArrayList<>());
                predList.add(currTask);
                taskPredecessors.put(subsqTask, predList);

                // currTask successor is subsqTask
                List<StreamTask> succList = taskSuccessors.getOrDefault(currTask, new ArrayList<>());
                succList.add(subsqTask);
                taskSuccessors.put(currTask, succList);
            });
        }
    }

    // ------------------------------------------------------------------------
    //  stats methods
    // ------------------------------------------------------------------------

    /*
     * Updates taskQueueSize map.
     *
     * It subtracts {@param task} inEvents from the aggregate of
     * predecessors outEvents.
     *
     * {@param task} the task to estimate the queue size for.
     * @returns the queue size.
     */
    private long updateQueueSize(StreamTask task) {
        List<StreamTask> predsList = taskPredecessors.get(task);
        if (predsList == null) {
            /*
             * This only happens if the task is localized on a different node
             * or it is a source operator.
             */
            taskQueueSize.put(task, (long) 0);
            return 0;
        }
        /* Fetch predecessor metrics */
        long outEvents = predsList.
                stream().
                // For chain operators, we need to find the last task in the chain.
                        map(StreamTask::getLastOperator).
                        map(predOp -> ((OperatorMetricGroup) predOp.getMetricGroup()).getIOMetricGroup()).
                        mapToLong(ioMetricGroup ->
                                ioMetricGroup.getNumRecordsOutCounter().getCount() +
                                        ioMetricGroup.getNumWatermarksOut().getCount() +
                                        ioMetricGroup.getNumLatencyMarkersOut().getCount())
                .sum();

        /* Fetch successor metrics */
        // We only care about first operator in the current task to determine buffer size.
        OperatorIOMetricGroup operatorIOMetricGroup =
                ((OperatorMetricGroup) task.getHeadOperator().getMetricGroup()).getIOMetricGroup();

        long queueSize = outEvents -
                (operatorIOMetricGroup.getNumRecordsInCounter().getCount() +
                        operatorIOMetricGroup.getNumWatermarksIn().getCount() +
                        operatorIOMetricGroup.getNumLatencyMarkersIn().getCount());
        taskQueueSize.put(task, queueSize);
        return queueSize;
    }

    private void printQueryStats() {
        getScheduledTasksList().stream()
                .filter(task -> task.getHeadOperator() instanceof StreamSink)
                .forEach(task -> {
                    /* Print metrics per query, starting from Sink operator. */
                    StreamSink operator = (StreamSink) task.getHeadOperator();
                    System.out.println("Query: " + operator.getOperatorConfig().getOperatorName());
                    System.out.println("===============");

                    /* Number of processed events */
                    System.out.println("Number of processed events at sink: " +
                            ((OperatorMetricGroup) (operator.getMetricGroup())).getIOMetricGroup().getNumRecordsInCounter().getCount());

                    /* Number of processed watermarks */
                    System.out.println("Number of processed watermarks: " + operator.getProcessedWatermarksCount().getCount());

                    /* latency */
                    operator.getLatencyStats().getLatencyStats().forEach(latencyStat -> {
                        System.out.println("Count\t\tLatency: " + latencyStat.getCount());
                        System.out.println("Max\t\tLatency: " + latencyStat.getStatistics().getMax());
                        System.out.println("Min\t\tLatency: " + latencyStat.getStatistics().getMin());
                        System.out.println("Avg\t\tLatency: " + latencyStat.getStatistics().getMean());
                        System.out.println("Dev\t\tLatency: " + latencyStat.getStatistics().getStdDev());
                        System.out.println();
                    });

                    /* Blocking Latency */
                    LatencyStats blockingLatencies = operator.getBlockingLatencyStats();
                    blockingLatencies.getLatencyStats().forEach(latencyStat -> {
                        System.out.println("Count\t\tBlocking Latency: " + latencyStat.getCount());
                        System.out.println("Max\t\tBlocking Latency: " + latencyStat.getStatistics().getMax());
                        System.out.println("Min\t\tBlocking Latency: " + latencyStat.getStatistics().getMin());
                        System.out.println("Avg\t\tBlocking Latency: " + latencyStat.getStatistics().getMean());
                        System.out.println("Dev\t\tBlocking Latency: " + latencyStat.getStatistics().getStdDev());
                        System.out.println();
                    });


                    /* Emitting Watermark propagation delay */
                    Histogram watPropDelay = ((StreamSink) task.getHeadOperator()).getEmittingWatermarkPropagationDelay();
                    System.out.println("Count\t\tEmitting Watermark Prop Delay: " + watPropDelay.getCount());
                    System.out.println("Maximum\t\tEmitting Watermark Prop Delay: " + watPropDelay.getStatistics().getMax());
                    System.out.println("Minimum\t\tEmitting Watermark Prop Delay: " + watPropDelay.getStatistics().getMin());
                    System.out.println("Average\t\tEmitting Watermark Prop Delay: " + watPropDelay.getStatistics().getMean());
                    System.out.println("Deviation\t\tEmitting Watermark Prop Delay: " + watPropDelay.getStatistics().getStdDev());
                    System.out.println();


                    /* Emmitting Watermark Arrival Delay */
                    StreamTask currTask = task;
                    while (currTask != null) {
                        // only at window operators
                        if (currTask.getHeadOperator() instanceof WindowOperator) {
                            Histogram emissionDelay =
                                    ((WindowOperator) currTask.getHeadOperator()).getEmittingWatermarkDelay();
                            System.out.println("Count\t\tEmitting Watermark Arrival Delay: " + emissionDelay.getCount());
                            System.out.println("Max\t\tEmitting Watermark Arrival Delay: " + emissionDelay.getStatistics().getMax());
                            System.out.println("Min\t\tEmitting Watermark Arrival Delay: " + emissionDelay.getStatistics().getMin());
                            System.out.println("Avg\t\tEmitting Watermark Arrival Delay: " + emissionDelay.getStatistics().getMean());
                            System.out.println("Dev\t\tEmitting Watermark Arrival Delay: " + emissionDelay.getStatistics().getStdDev());
                            System.out.println();
                        }
                        currTask = getPredecessor(currTask);
                    }


                    /* Slowdown */
                    double blockingLatency = 0;
                    blockingLatency = operator.getBlockingLatencyStats().getLatencyStats().stream().
                            mapToDouble(stat -> stat.getStatistics().getMean()).average().getAsDouble();


                    long totalPipelineProcTime = 0;
                    currTask = task;

                    while (currTask != null) {
                        // estimate proccTime
                        if (currTask.getHeadOperator() instanceof WindowOperator) {
                            totalPipelineProcTime += ((WindowOperator) (currTask.getHeadOperator())).getEmittingWatermarkDelay().getStatistics().getMean();
                        } else {
                            totalPipelineProcTime += ((OperatorMetricGroup) currTask.getHeadOperator().getMetricGroup())
                                    .getIOMetricGroup().getTotalProcessingTime().getStatistics().getMean();
                        }
                        currTask = getPredecessor(currTask);
                    }
                    System.out.printf("Average Query Slowdown with blocking-latency: %.4f\n\n", totalPipelineProcTime / blockingLatency * 1.0);
                    System.out.println("\n\n");
                });
    }

    private void printSchedulerStats() {
        // Cycles counter
        System.out.println("Number of cycles: " + numOfCyclesCount.getCount());

        // Scheduler overhead Histogram
        System.out.println("Count\t\tscheduler overhead: " + schedulerOverheadHisto.getCount());
        System.out.println("Maximum\t\tscheduler overhead: " + schedulerOverheadHisto.getStatistics().getMax());
        System.out.println("Minimum\t\tscheduler overhead: " + schedulerOverheadHisto.getStatistics().getMin());
        System.out.println("Average\t\tscheduler overhead: " + schedulerOverheadHisto.getStatistics().getMean());
        System.out.println("Deviation\t\tscheduler overhead: " + schedulerOverheadHisto.getStatistics().getStdDev());
        System.out.println();

        // Sleep duration per cycle
        System.out.println("Count\t\tsleep duration per cycle: " + sleepDurationHisto.getCount());
        System.out.println("Maximum\t\tsleep duration per cycle: " + sleepDurationHisto.getStatistics().getMax());
        System.out.println("Minimum\t\tsleep duration per cycle: " + sleepDurationHisto.getStatistics().getMin());
        System.out.println("Average\t\tsleep duration per cycle: " + sleepDurationHisto.getStatistics().getMean());
        System.out.println("Deviation\t\tsleep duration per cycle: " + sleepDurationHisto.getStatistics().getStdDev());
        System.out.println();

        // Number of scheduled tasks
        System.out.println("Count\t\tNum of scheduled tasks per cycle: " + numberOfRunningTasksHisto.getCount());
        System.out.println("Maximum\t\tNum of scheduled tasks per cycle: " + numberOfRunningTasksHisto.getStatistics().getMax());
        System.out.println("Minimum\t\tNum of scheduled tasks per cycle: " + numberOfRunningTasksHisto.getStatistics().getMin());
        System.out.println("Average\t\tNum of scheduled tasks per cycle: " + numberOfRunningTasksHisto.getStatistics().getMean());
        System.out.println("Deviation\t\tNum of scheduled tasks per cycle: " + numberOfRunningTasksHisto.getStatistics().getStdDev());
        System.out.println();

        // num of events of all tasks
        System.out.println("Count\t\tnum of events of all tasks per cycle: " + numOfEventsOfAllTasksHisto.getCount());
        System.out.println("Maximum\t\tnum of events of all tasks per cycle: " + numOfEventsOfAllTasksHisto.getStatistics().getMax());
        System.out.println("Minimum\t\tnum of events of all tasks per cycle: " + numOfEventsOfAllTasksHisto.getStatistics().getMin());
        System.out.println("Average\t\tnum of events of all tasks per cycle: " + numOfEventsOfAllTasksHisto.getStatistics().getMean());
        System.out.println("Deviation\t\tnum of events of all tasks per cycle: " + numOfEventsOfAllTasksHisto.getStatistics().getStdDev());
        System.out.println();

        // num of events of scheduled tasks
        System.out.println("Count\t\tnum of events of scheduled tasks per cycle: " + numOfEventsOfScheduledTasksHisto.getCount());
        System.out.println("Maximum\t\tnum of events of scheduled tasks per cycle: " + numOfEventsOfScheduledTasksHisto.getStatistics().getMax());
        System.out.println("Minimum\t\tnum of events of scheduled tasks per cycle: " + numOfEventsOfScheduledTasksHisto.getStatistics().getMin());
        System.out.println("Average\t\tnum of events of scheduled tasks per cycle: " + numOfEventsOfScheduledTasksHisto.getStatistics().getMean());
        System.out.println("Deviation\t\tnum of events of scheduled tasks per cycle: " + numOfEventsOfScheduledTasksHisto.getStatistics().getStdDev());
        System.out.println();

        // num of events of processed tasks
        System.out.println("Count\t\tnum of events of processed tasks per cycle: " + numOfEventsProcessedHisto.getCount());
        System.out.println("Maximum\t\tnum of events of processed tasks per cycle: " + numOfEventsProcessedHisto.getStatistics().getMax());
        System.out.println("Minimum\t\tnum of events of processed tasks per cycle: " + numOfEventsProcessedHisto.getStatistics().getMin());
        System.out.println("Average\t\tnum of events of processed tasks per cycle: " + numOfEventsProcessedHisto.getStatistics().getMean());
        System.out.println("Deviation\t\tnum of events of processed tasks per cycle: " + numOfEventsProcessedHisto.getStatistics().getStdDev());
        System.out.println();

        // starvation
        System.out.println("Count\t\tstarvation: " + starvationHisto.getCount());
        System.out.println("Maximum\t\tstarvation: " + starvationHisto.getStatistics().getMax());
        System.out.println("Minimum\t\tstarvation: " + starvationHisto.getStatistics().getMin());
        System.out.println("Average\t\tstarvation: " + starvationHisto.getStatistics().getMean());
        System.out.println("Deviation\t\tstarvation: " + starvationHisto.getStatistics().getStdDev());
        System.out.println();
    }

    // ------------------------------------------------------------------------
    //  helpers
    // ------------------------------------------------------------------------

    private final Comparator<StreamTask> taskWithPrioritiesComparator = new Comparator<StreamTask>() {
        @Override
        public int compare(StreamTask st1, StreamTask st2) {
            if (taskPriorities.getOrDefault(st1, 0.0).equals(taskPriorities.getOrDefault(st2, 0.0))) {
                return st1.getName().compareTo(st2.getName());
            }
            if (highestPriorityFirst) {
                return Double.compare(taskPriorities.getOrDefault(st2, 0.0), taskPriorities.getOrDefault(st1, 0.0));
            } else {
                return Double.compare(taskPriorities.getOrDefault(st1, 0.0), taskPriorities.getOrDefault(st2, 0.0));
            }
        }
    };

    @Override
    public void printStats() {
        printQueryStats();
        printSchedulerStats();
    }

    protected final StreamTask getPredecessor(StreamTask task) {
        List<StreamTask> preds = taskPredecessors.get(task);
        if (preds == null || preds.size() < 1) {
            return null;
        }
        return preds.get(0);
    }

    protected final StreamTask getSuccessor(StreamTask task) {
        List<StreamTask> succ = taskSuccessors.get(task);
        if (succ == null || succ.size() < 1) {
            return null;
        }
        return succ.get(0);
    }
}

