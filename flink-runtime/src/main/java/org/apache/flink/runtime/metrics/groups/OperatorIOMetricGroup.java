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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.metrics.*;
import org.apache.flink.runtime.metrics.MetricNames;

/**
 * Metric group that contains shareable pre-defined IO-related metrics. The metrics registration is
 * forwarded to the parent operator metric group.
 */
public class OperatorIOMetricGroup extends ProxyMetricGroup<OperatorMetricGroup> {

	private final Counter numRecordsIn;
	private final Counter numRecordsOut;

	private final Counter numWatermarksIn;
	private final Counter numWatermarksOut;

	private final Counter numLatencyMarkersIn;
	private final Counter numLatencyMarkersOut;

	private final Meter numRecordsInRate;
	private final Meter numRecordsOutRate;

	private final Histogram totalProcessingTime;

	public OperatorIOMetricGroup(OperatorMetricGroup parentMetricGroup) {
		super(parentMetricGroup);
		numRecordsIn = parentMetricGroup.counter(MetricNames.IO_NUM_RECORDS_IN);
		numRecordsOut = parentMetricGroup.counter(MetricNames.IO_NUM_RECORDS_OUT);

		numWatermarksIn = parentMetricGroup.counter(MetricNames.IO_NUM_WATERMARKS_IN);
		numWatermarksOut = parentMetricGroup.counter(MetricNames.IO_NUM_WATERMARKS_OUT);

		numLatencyMarkersIn = parentMetricGroup.counter(MetricNames.IO_NUM_LATENCY_MARKERS_IN);
		numLatencyMarkersOut = parentMetricGroup.counter(MetricNames.IO_NUM_LATENCY_MARKERS_OUT);

		numRecordsInRate = parentMetricGroup.meter(MetricNames.IO_NUM_RECORDS_IN_RATE, new MeterView(numRecordsIn, 60));
		numRecordsOutRate = parentMetricGroup.meter(MetricNames.IO_NUM_RECORDS_OUT_RATE, new MeterView(numRecordsOut, 60));
		totalProcessingTime = parentMetricGroup.histogram(MetricNames.IO_TOTAL_PROCESSING_TIME, new SimpleTimeHistogram());
	}

	public Counter getNumRecordsInCounter() {
		return numRecordsIn;
	}

	public Counter getNumRecordsOutCounter() {
		return numRecordsOut;
	}

	public Counter getNumWatermarksIn() {
		return numWatermarksIn;
	}

	public Counter getNumWatermarksOut() {
		return numWatermarksOut;
	}

	public Counter getNumLatencyMarkersIn() {
		return numLatencyMarkersIn;
	}

	public Counter getNumLatencyMarkersOut() {
		return numLatencyMarkersOut;
	}

	public Meter getNumRecordsInRateMeter() {
		return numRecordsInRate;
	}

	public Meter getNumRecordsOutRate() {
		return numRecordsOutRate;
	}

	public Histogram getTotalProcessingTime() { return totalProcessingTime; }
	/**
	 * Causes the containing task to use this operators input record counter.
	 */
	public void reuseInputMetricsForTask() {
		TaskIOMetricGroup taskIO = parentMetricGroup.parent().getIOMetricGroup();
		taskIO.reuseRecordsInputCounter(this.numRecordsIn);

	}

	/**
	 * Causes the containing task to use this operators output record counter.
	 */
	public void reuseOutputMetricsForTask() {
		TaskIOMetricGroup taskIO = parentMetricGroup.parent().getIOMetricGroup();
		taskIO.reuseRecordsOutputCounter(this.numRecordsOut);
	}

	private final class SimpleTimeHistogram implements Histogram {
		private final HistogramStatistics histogramStatistics;

		/** Number of entries */
		private long count = 0;

		/** Aggregate processing time total */
		private long totalProcessingTime = 0;

		/** Minimum processing time. */
		private long minProcessingTime = Long.MAX_VALUE;

		/** Maximum processing time. */
		private long maxProcessingTime = 0;

		SimpleTimeHistogram(){
			histogramStatistics = new SimpleTimeHistogramStatistics();
		}

		/**
		 * Core methods
		 */
		@Override
		public void update(long l) {
			minProcessingTime = Math.min(minProcessingTime, l);
			maxProcessingTime = Math.max(maxProcessingTime, l);

			totalProcessingTime += l;
			count++;
		}

		@Override
		public long getCount() {
			return count;
		}

		@Override
		public HistogramStatistics getStatistics() {
			return histogramStatistics;
		}

		private final class SimpleTimeHistogramStatistics extends HistogramStatistics {

			@Override
			public double getQuantile(double v) {
				return 0;
			}

			@Override
			public long[] getValues() {
				return new long[0];
			}

			@Override
			public int size() {
				return (int) getCount();
			}

			@Override
			public double getMean() {
				if (count == 0) {
					return 0;
				}
				return totalProcessingTime * 1.0 / getCount();
			}

			@Override
			public double getStdDev() {
				return 0;
			}

			@Override
			public long getMax() {
				return maxProcessingTime;
			}

			@Override
			public long getMin() {
				return minProcessingTime;
			}
		}
	}
}
