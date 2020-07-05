/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.util.LatencyStats;

import java.util.List;
import java.util.Locale;

/**
 * A {@link StreamOperator} for executing {@link SinkFunction SinkFunctions}.
 */
@Internal
public class StreamSink<IN> extends AbstractUdfStreamOperator<Object, SinkFunction<IN>>
		implements OneInputStreamOperator<IN, Object> {

	private static final long serialVersionUID = 1L;

	private transient SimpleContext sinkContext;

	private transient LatencyStats blockingLatencyStats;

	private transient Counter processedWatermarksCount;

	private transient Histogram emittingWatermarkPropagationDelay;


	/** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
	private long currentWatermark = Long.MIN_VALUE;

	public StreamSink(SinkFunction<IN> sinkFunction) {
		super(sinkFunction);
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.sinkContext = new SimpleContext<>(getProcessingTimeService());
		// Setup blocking latency stats
		this.processedWatermarksCount = metrics.parent().counter("numberOfProcessedWatermarks");
		// Setup emitting latency propagation delay
		this.emittingWatermarkPropagationDelay = metrics.parent().histogram("emittingWatermarkPropagationDelay",
				new DescriptiveStatisticsHistogram(16384));

        this.blockingLatencyStats = new LatencyStats(
                metrics.parent().parent().addGroup("blockingLatency"),
                getContainingTask().getEnvironment().getTaskManagerInfo()
                        .getConfiguration().getInteger(MetricOptions.LATENCY_HISTORY_SIZE),
                getContainingTask().getIndexInSubtaskGroup(),
                getOperatorID(),
                LatencyStats.Granularity.OPERATOR);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		sinkContext.element = element;
		userFunction.invoke(element.getValue(), sinkContext);
	}

	@Override
	protected void reportOrForwardLatencyMarker(LatencyMarker marker) {
		// all operators are tracking latencies
		if (marker.isBlocking()) {
			this.blockingLatencyStats.reportLatency(marker);
		} else {
			this.latencyStats.reportLatency(marker);
		}
		// sinks don't forward latency markers
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		processedWatermarksCount.inc();
		this.currentWatermark = mark.getTimestamp();

		// update emitting propagation delay watermarks.
		if (mark.isEmitting()) {
			emittingWatermarkPropagationDelay.update(System.currentTimeMillis() - mark.getIngestionTimestamp());
		}
	}

	public LatencyStats getBlockingLatencyStats() {
		return this.blockingLatencyStats;
	}

	public Counter getProcessedWatermarksCount() {
		return processedWatermarksCount;
	}

	public Histogram getEmittingWatermarkPropagationDelay () {
		return emittingWatermarkPropagationDelay;
	}

	private class SimpleContext<IN> implements SinkFunction.Context<IN> {

		private StreamRecord<IN> element;

		private final ProcessingTimeService processingTimeService;

		public SimpleContext(ProcessingTimeService processingTimeService) {
			this.processingTimeService = processingTimeService;
		}

		@Override
		public long currentProcessingTime() {
			return processingTimeService.getCurrentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return currentWatermark;
		}

		@Override
		public Long timestamp() {
			if (element.hasTimestamp()) {
				return element.getTimestamp();
			}
			return null;
		}
	}
}
