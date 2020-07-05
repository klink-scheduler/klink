package org.apache.flink.runtime.taskexecutor.scheduler;

import org.apache.flink.runtime.metrics.groups.TaskSchedulerMetricGroup;
import org.apache.flink.runtime.taskmanager.Task;

import java.io.Serializable;


/**
 * {@link TaskScheduler} runtime executor. This class acts as a wrapper class to
 * {@link TaskScheduler} wrapper on the runtime level.
 */
public interface RuntimeTaskScheduler extends Runnable, Serializable {

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    /**
     * Initializes the TaskScheduler. Sets the schedulerPolicy.
     */
    void setup(TaskSchedulerMetricGroup metricGroup, ClassLoader classLoader,
               String taskSchedulerClassName, String taskSchedulerPolicy);

    /**
     * This method is called after all tasks have been closed.
     *
     * <p>The method is expected to close all contained components. Exceptions should be propagated.
     */
    void stop();

    // ------------------------------------------------------------------------
    //  wrappers
    // ------------------------------------------------------------------------

    /**
     * Add a {@link Task} to the {@link RuntimeTaskScheduler}.
     *
     * @param task the submitted task
     * @return success output of this operation
     */
    boolean addTask(Task task);

    /**
     * Remove a {@link Task} from the {@link RuntimeTaskScheduler}.
     *
     * @param task the submitted task
     * @return success output of this operation
     */
    boolean removeTask(Task task);

    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------

    boolean isSetup();
}
