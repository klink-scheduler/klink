package org.apache.flink.runtime.metrics.scope;

import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskSchedulerMetricGroup;

/**
 * The scope format for the {@link TaskSchedulerMetricGroup}.
 */
public class TaskSchedulerScopeFormat extends ScopeFormat {

    public TaskSchedulerScopeFormat(String format, TaskScopeFormat parentFormat) {
        super(format, parentFormat, new String[]{
                SCOPE_HOST,
                SCOPE_TASKMANAGER_ID,
                SCOPE_SCHEDULER_NAME,
        });
    }

    public String[] formatScope(TaskManagerMetricGroup parent, String schedulerName) {

        final String[] template = copyTemplate();
        final String[] values = {
                parent.hostname(),
                parent.taskManagerId(),
                valueOrNull(schedulerName)
        };
        return bindVariables(template, values);
    }
}
