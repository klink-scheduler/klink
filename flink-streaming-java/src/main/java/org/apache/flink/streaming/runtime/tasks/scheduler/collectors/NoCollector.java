package org.apache.flink.streaming.runtime.tasks.scheduler.collectors;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;

/* Empty collector */
public class NoCollector implements SchedulerCollector {
	@Override
	public void collect(StreamRecord streamRecord) {
		// DO NOTHING
	}

	@Override
	public void collect(Watermark watermark) {
		// DO NOTHING
	}

	@Override
	public int numOfCollectedRecords() {
		return 0;
	}

	@Override
	public int numOfCollectedWatermarks() {
		return 0;
	}

	@Override
	public List<StreamRecord> collectedRecords() {
		return null;
	}

	@Override
	public List<Watermark> collectedWatermarks() {
		return null;
	}

	@Override
	public void reset() {
		// DO NOTHING
	}
}
