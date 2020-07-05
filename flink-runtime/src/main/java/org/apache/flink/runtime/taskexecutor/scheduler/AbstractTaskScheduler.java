/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor.scheduler;

import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.groups.TaskSchedulerMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This is the abstract base class for TaskSchedulers.
 * Concrete schedulers extend this class, for example the streaming and batch tasks.
 *
 * <p>The {@link RuntimeTaskScheduler} invokes the {@link #invoke()} method to start the scheduler.
 *
 * <p>All classes that extend must offer a constructor {@code TaskScheduler(taskSchedulerPolicyIndex)}.
 */
public abstract class AbstractTaskScheduler implements TaskScheduler {

    protected final Logger LOG = LoggerFactory.getLogger(AbstractTaskScheduler.class);
    /**
     * Concurrent List containing tasks executing on the TaskManager.
     */
    protected final CopyOnWriteArrayList<AbstractInvokable> scheduledTasks = new CopyOnWriteArrayList<>();

    /**
     * Metric group for the scheduler.
     */
    protected transient TaskSchedulerMetricGroup metricGroup;

    private String taskSchedulerPolicy;

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    @Override
    public void setup(TaskSchedulerMetricGroup metricGroup, String taskSchedulerPolicy) {
        this.metricGroup = metricGroup;
        this.taskSchedulerPolicy = taskSchedulerPolicy;
    }

    @Override
    public void close(){
        metricGroup.close();
    }

    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------

    public TaskSchedulerMetricGroup getMetricGroup() {
        return metricGroup;
    }

    public String getSchedulerName() {
        return taskSchedulerPolicy;
    }
}
