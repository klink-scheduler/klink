package org.apache.flink.runtime.taskexecutor.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.FlinkException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.Collection;

public class TaskSchedulerUtils {

    // --------------------------------------------------------------------------------------------
    //  Static utilities
    // --------------------------------------------------------------------------------------------

    /**
     * Instantiates the given scheduler class.
     *
     * <p>The method will first try to instantiate the scheduler via a constructor accepting the policy.</p>
     *
     * @param userCodeClassLoader The classloader to load the class through.
     * @param className           The name of the class to load.
     * @return The instantiated scheduler object.
     * @throws Throwable Forwards all exceptions that happen during initialization of the task.
     *                   Also throws an exception if the task class misses the necessary constructor.
     */
    public static AbstractTaskScheduler loadAndInstantiateTaskScheduler(
            ClassLoader userCodeClassLoader, String className) throws Exception {
        final Class<? extends AbstractTaskScheduler> taskSchedulerClass;
        try {
            taskSchedulerClass =
                    Class.forName(className, true, userCodeClassLoader)
                            .asSubclass(AbstractTaskScheduler.class);
        } catch (Throwable t) {
            throw new Exception("Could not load the TaskScheduler " + className, t);
        }
        Constructor<? extends AbstractTaskScheduler> statelessCtor;

        try {
            statelessCtor = taskSchedulerClass.getConstructor();
        } catch (NoSuchMethodException ee) {
            throw new FlinkException("Task misses proper constructor for TaskScheduler", ee);
        }

        // instantiate the class
        try {
            return statelessCtor.newInstance();
        } catch (InvocationTargetException e) {
            // directly forward exceptions from the eager initialization
            throw e;
        } catch (Exception e) {
            throw new FlinkException("Could not instantiate the TaskScheduler class.", e);
        }
    }

    public static ClassLoader createClassLoader(JobID jobId, ExecutionAttemptID executionId,
            Collection<PermanentBlobKey> requiredJarFiles, Collection<URL> requiredClasspaths,
            LibraryCacheManager libraryCache) throws Exception {

        // triggers the download of all missing jar files from the job manager
        libraryCache.registerTask(jobId, executionId, requiredJarFiles, requiredClasspaths);

        ClassLoader userCodeClassLoader = libraryCache.getClassLoader(jobId);
        if (userCodeClassLoader == null) {
            throw new Exception("No user code classloader available.");
        }
        return userCodeClassLoader;
    }
}
