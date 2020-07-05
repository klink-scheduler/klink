package org.apache.flink.streaming.runtime.tasks.scheduler.collectors;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;

/**
 * StreamTaskScheduler event's collector.
 *
 * <p>Different scheduling algorithm may require different collector algorithms.
 * The methods of this interface are divided into collectors and getters.
 * This class is a part of scheduler's monitor. </p>
 */

public interface SchedulerCollector {

	/**
	 * Collect a streamRecord.
	 *
	 * @param streamRecord The record to be collected.
	 */
	void collect(StreamRecord streamRecord);

	/**
	 * Collect a watermark.
	 *
	 * @param watermark The watermark to be collected.
	 */
	void collect(Watermark watermark);

	/**
	 * Number of collected records.
	 *
	 * @return num of collected records
	 */
	int numOfCollectedRecords();

	/**
	 * Number of collected watermarks.
	 *
	 * @return num of collected watermarks.
	 */
	int numOfCollectedWatermarks();

	/**
	 * Returns list of collected records.
	 *
	 * @return list containing collected records
	 */
	List<StreamRecord> collectedRecords();

	/**
	 * Returns list of collected watermarks.
	 *
	 * @return list containing collected watermarks
	 */
	List<Watermark> collectedWatermarks();

	/**
	 * Resets collected data.
	 *
	 * This method is used to reset collected data.
	 * It can be invoked every other scheduling round.
	 */

	void reset();
}
