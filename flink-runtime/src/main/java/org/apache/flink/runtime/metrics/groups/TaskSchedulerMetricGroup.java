package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Metric group that contains shareable pre-defined scheduler-related metrics. The metrics registration is
 * forwarded to the parent TaskManager metric group.
 */
@Internal
public class TaskSchedulerMetricGroup extends ComponentMetricGroup<TaskManagerMetricGroup> {
    private final String schedulerName;

    public TaskSchedulerMetricGroup(MetricRegistry registry, TaskManagerMetricGroup parent, String schedulerName) {
        super(registry,
                registry.getScopeFormats().getSchedulerFormat().formatScope(checkNotNull(parent), schedulerName),
                parent);
        this.schedulerName = schedulerName;
    }

    // ------------------------------------------------------------------------

    public final TaskManagerMetricGroup parent() {
        return parent;
    }

    @Override
    protected QueryScopeInfo.TaskSchedulerQueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
        return new QueryScopeInfo.TaskSchedulerQueryScopeInfo(filter.filterCharacters(this.schedulerName), parent().taskManagerId());
    }
    
    // ------------------------------------------------------------------------
    //  Component Metric Group Specifics
    // ------------------------------------------------------------------------

    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put(ScopeFormat.SCOPE_SCHEDULER_NAME, String.valueOf(schedulerName));
        // we don't enter the subtask_index as the task group does that already
    }

    @Override
    protected Iterable<? extends ComponentMetricGroup> subComponents() {
        return Collections.emptyList();
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "taskscheduler";
    }
}
