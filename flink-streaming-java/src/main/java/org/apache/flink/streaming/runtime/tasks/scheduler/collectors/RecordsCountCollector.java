package org.apache.flink.streaming.runtime.tasks.scheduler.collectors;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;

public class RecordsCountCollector implements SchedulerCollector {

	private int collectedEvents;

	public RecordsCountCollector() {
		this.collectedEvents = 0;
	}

	@Override
	public void collect(StreamRecord streamRecord) {
		collectedEvents++;
	}

	@Override
	public void collect(Watermark watermark) {}

	@Override
	public int numOfCollectedRecords() {
		return collectedEvents;
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
		this.collectedEvents = 0;
	}

}
