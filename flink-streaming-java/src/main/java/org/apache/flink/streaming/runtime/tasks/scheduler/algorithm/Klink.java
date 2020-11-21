package org.apache.flink.streaming.runtime.tasks.scheduler.algorithm;

import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskSchedulerMetricGroup;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.scheduler.SchedulerMonitor;
import org.apache.flink.streaming.runtime.tasks.scheduler.StreamTasksListener;
import org.apache.flink.streaming.runtime.tasks.scheduler.collectors.RecordsCountCollector;
import org.apache.flink.streaming.runtime.tasks.scheduler.diststore.DistStoreManager;
import org.apache.flink.streaming.runtime.tasks.scheduler.diststore.NetDelaySSStore;
import org.apache.flink.streaming.runtime.tasks.scheduler.diststore.WindowDistStore;

import java.util.*;

public class Klink extends AbstractPriorityAlgorithm {

    private final Map<StreamTask, WindowOperator> taskToWindowTask;
    private final DistStoreManager distStoreManager;

    Klink(StreamTasksListener tasksListener, TaskSchedulerMetricGroup metricGroup, int numOfCores) {
        super(tasksListener, metricGroup, numOfCores, false);
        this.taskToWindowTask = new HashMap<>(4);
        this.distStoreManager = new DistStoreManager(DistStoreManager.DistStoreType.NET_DELAY);
    }

    /* Associate Each Task to the WindowOperator. */
    @Override
    protected void setupChange(List<StreamTask> tasks, long cycleNumber) {
        taskToWindowTask.clear();

        for (StreamTask sourceTask : tasks) {
            if (!(sourceTask instanceof SourceStreamTask)) {
                continue;
            }

            /* Associate each task with a window operator. */
            Queue<StreamTask> tasksQueue = new LinkedList<>();
            StreamTask currTask = sourceTask;

            while (currTask != null) {
                // Add to queue
                tasksQueue.offer(currTask);

                if (currTask.getHeadOperator() instanceof WindowOperator) {
                    // We found the window, now build association.
                    distStoreManager.createWindowDistStore((WindowOperator) currTask.getHeadOperator());
                    while (!tasksQueue.isEmpty()) {
                        taskToWindowTask.put(tasksQueue.poll(), (WindowOperator) currTask.getHeadOperator());
                    }
                }
                // Get successor
                currTask = getSuccessor(currTask);
            }
        }
    }

    @Override
    protected void computePriorities(List<StreamTask> tasks, Map<StreamTask, Double> taskPriorities, long cycleNumber) {
        for (StreamTask sinkTask : tasks) {
            if (!(sinkTask.getHeadOperator() instanceof StreamSink)) {
                continue;
            }

            // First pass, alternatively, we can map queries to window.
            StreamTask currTask = sinkTask;

            /*
             * If the watermarks in the pipeline are associated with multiple windows, it implies that
             * one window is overdue and thus should be given utmost priority.
             *
             * If the watermarks in the pipeline are associated with a window, each operator is given
             * the priority discussed in the paper. The closer it is to the window boundary
             * the higher the priority.
             * To some extent, we would be scheduling paths here so we should take other factors into consideration
             * like starvation. However, starvation alone is not enough as all operators int he same path
             * have an equivalent value of starvation. Perhaps Queue size?
             */

            long largestWatermark = 0;
            Window largestWindow = null;

            // Need to cache results as scheduling decision change.
            Map<StreamTask, Window> taskToWindow = new HashMap<>();

            long prevWatermark = 0;
            while (currTask != null) {
                if (!(currTask instanceof OneInputStreamTask)) {
                    // Do not schedule source operators.
                    currTask = getPredecessor(currTask);
                    continue;
                }

                long watermark = ((OneInputStreamTask) currTask).getWatermark();
                WindowOperator windowOperator = taskToWindowTask.get(currTask);

                /*
                 * If early operators in the pipeline don't have an assigned watermark,
                 * assign it the same one in the next stages.
                 */
                if (watermark <= 0) {
                    watermark = prevWatermark;
                }

                if (windowOperator == null || watermark <= 0) {
                    // Either a high priority operator or watermark was not generated yet.
                    currTask = getPredecessor(currTask);
                    continue;
                }

                // update largest watermark
                largestWatermark = Math.max(largestWatermark, watermark);

                // Get associated windows with that watermark
                List<Window> assignedWindows = windowOperator.getWindows(watermark);
                for (Window window : assignedWindows) {
                    // update largest window
                    if (largestWindow == null) {
                        largestWindow = window;
                    } else if (window.maxTimestamp() > largestWindow.maxTimestamp()) {
                        largestWindow = window;
                    }

                    taskToWindow.put(currTask, window);
                }
                currTask = getPredecessor(currTask);
                prevWatermark = watermark;
            }

            currTask = sinkTask;
            while (currTask != null) {
                // Ignore source operators
                if (currTask instanceof SourceStreamTask) {
                    break;
                }
                Window window = taskToWindow.getOrDefault(currTask, null);

                double priority;
                if (taskQueueSize.get(currTask) == 0) {
                    // No events to schedule
                    priority = Integer.MAX_VALUE;
                    // Case 3
                } else if (window == null) {
                    priority = 0;
                    // Case 2
                } else if (!window.equals(largestWindow)) {
                    priority = 1;
                    // Case 1
                } else {
                    // minimize watermark difference / starvation
                    priority = (window.maxTimestamp() - largestWatermark) /
                            (1.0 * (cycleNumber - lastTaskCycle.get(currTask)) - distStoreManager.getMeanDelay());
                }
                taskPriorities.put(currTask, priority);
                currTask = getPredecessor(currTask);
            }
        }
    }

    @Override
    protected double computePriority(StreamTask task, long cycleNumber) {
        throw new UnsupportedOperationException("This method should not have been called!");
    }

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

    @Override
    SchedulerMonitor createMonitor(StreamTask streamTask) {
        return new SchedulerMonitor(new RecordsCountCollector());
    }
}
