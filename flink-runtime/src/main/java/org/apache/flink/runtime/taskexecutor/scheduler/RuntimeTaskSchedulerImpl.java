package org.apache.flink.runtime.taskexecutor.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.metrics.groups.TaskSchedulerMetricGroup;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.Collection;

import static org.apache.flink.runtime.taskexecutor.scheduler.TaskSchedulerUtils.loadAndInstantiateTaskScheduler;

/**
 * Implementation of {@link RuntimeTaskScheduler}.
 */
public class RuntimeTaskSchedulerImpl implements RuntimeTaskScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(RuntimeTaskScheduler.class);

    /**
     * The thread that executes the taskScheduler.
     */
    private Thread executingThread;

    /**
     * TaskScheduler instance
     */
    private TaskScheduler taskScheduler;

    private boolean isSetup = false;

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    @Override
    public void setup(TaskSchedulerMetricGroup metricGroup, ClassLoader classLoader,
                      String taskSchedulerClassName, String taskSchedulerPolicy) {
        if (isSetup) {
            LOG.warn("Attempting to setup the RuntimeTaskScheduler more than once");
            return;
        }

        LOG.info("Setting up RuntimeTaskScheduler");

        try {
            this.taskScheduler = loadAndInstantiateTaskScheduler(
                    classLoader, taskSchedulerClassName);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        isSetup = true;
        this.taskScheduler.setup(metricGroup, taskSchedulerPolicy);

        executingThread = new Thread(this);
        executingThread.setName("TaskScheduler: " + taskSchedulerPolicy);
        executingThread.start();
    }

    @Override
    public void run() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Invoking TaskScheduler instance");
        }

        try {
            taskScheduler.invoke();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        } finally {
            LOG.info("Stopping RuntimeTaskScheduler");
            stop();
        }
    }

    @Override
    public void stop() {
        try {
            if (taskScheduler != null) {
                taskScheduler.close();
            }
            executingThread.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ------------------------------------------------------------------------
    //  wrappers
    // ------------------------------------------------------------------------

    @Override
    public boolean addTask(Task task) {
        if (executingThread != null && !executingThread.isAlive()) {
            LOG.error("Attempting to add task to a dead scheduler");
            return false;
        }

        /* Race condition. Wait until the invokable is instantiated. */
        while (task.getInvokable() == null) ;

        return taskScheduler != null &&
                taskScheduler.addInvokable(task.getInvokable());
    }

    @Override
    public boolean removeTask(Task task) {
        if (executingThread != null && !executingThread.isAlive()) {
            LOG.error("Attempting to remove task from a dead scheduler");
            return false;
        }
        return taskScheduler != null &&
                taskScheduler.removeInvokable(task.getInvokable());
    }

    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------

    @Override
    public boolean isSetup(){
        return isSetup;
    }
}
