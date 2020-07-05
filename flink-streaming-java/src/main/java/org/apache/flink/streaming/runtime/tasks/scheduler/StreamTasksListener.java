package org.apache.flink.streaming.runtime.tasks.scheduler;

import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.List;

/**
 * Interface  running tasks
 */
public interface StreamTasksListener {

	/*
	 * @return list of all scheduled stream tasks
	 */
	List<StreamTask> getScheduledTasks();

	/*
	 * @return if there has been some changes
	 */
	boolean hasChanged();
}
