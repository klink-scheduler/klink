package org.apache.flink.runtime.taskexecutor.scheduler;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.groups.TaskSchedulerMetricGroup;

import java.io.Serializable;

/**
 * Basic interface for task scheduler. Implementers would extend
 * {@link org.apache.flink.runtime.taskexecutor.scheduler.AbstractTaskScheduler}
 * that scheduler threads.
 *
 * <p>The class {@link org.apache.flink.runtime.taskexecutor.scheduler.AbstractTaskScheduler}
 * offers default implementation for the lifecycle and properties methods.
 */
@PublicEvolving
public interface TaskScheduler extends Serializable {

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    /**
     * Initializes the scheduler. Sets the schedulerPolicy.
     */
    void setup(TaskSchedulerMetricGroup metricGroup, String taskSchedulerPolicy);

    /**
     * This method is called immediately before any elements are processed, it should contain the
     * scheduler's policy initialization logic.
     *
     * @throws java.lang.Exception An exception in this method causes the operator to fail.
     */
    void invoke() throws Exception;

    /**
     * This method is called after all tasks have been closed.
     *
     * <p>The method is expected to close all contained components. Exceptions should be propagated.
     *
     * @throws java.lang.Exception An exception in this method causes the scheduler to fail.
     */
    void close() throws Exception;

    // ------------------------------------------------------------------------
    //  accessors
    // ------------------------------------------------------------------------

    /**
     * Add a {@link AbstractInvokable} to the list of scheduled tasks.
     *
     * @param invokable the submitted task
     * @return success output of this operation
     */
    boolean addInvokable(AbstractInvokable invokable);

    /**
     * Remove a {@link AbstractInvokable} to the list of scheduled tasks.
     *
     * @param invokable the submitted task
     * @return success output of this operation
     */
    boolean removeInvokable(AbstractInvokable invokable);

    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------

    TaskSchedulerMetricGroup getMetricGroup();

    String getSchedulerName();
}
