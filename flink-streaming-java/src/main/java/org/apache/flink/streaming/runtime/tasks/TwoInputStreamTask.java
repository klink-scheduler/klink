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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTwoInputProcessor;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link StreamTask} for executing a {@link TwoInputStreamOperator}.
 */
@Internal
public class TwoInputStreamTask<IN1, IN2, OUT> extends StreamTask<OUT, TwoInputStreamOperator<IN1, IN2, OUT>> {

	private StreamTwoInputProcessor<IN1, IN2> inputProcessor;

	private volatile boolean running = true;

	private final WatermarkGauge input1WatermarkGauge;
	private final WatermarkGauge input2WatermarkGauge;
	private final MinWatermarkGauge minInputWatermarkGauge;

	private Histogram processingTimeHistogram;


	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * @param env The task environment for this task.
	 */
	public TwoInputStreamTask(Environment env) {
		super(env);
		input1WatermarkGauge = new WatermarkGauge();
		input2WatermarkGauge = new WatermarkGauge();
		minInputWatermarkGauge = new MinWatermarkGauge(input1WatermarkGauge, input2WatermarkGauge);
	}

	@Override
	public void init() throws Exception {
		LOG.info("TwoInputStreamTask From " + Thread.currentThread().getName());

		StreamConfig configuration = getConfiguration();
		ClassLoader userClassLoader = getUserCodeClassLoader();

		TypeSerializer<IN1> inputDeserializer1 = configuration.getTypeSerializerIn1(userClassLoader);
		TypeSerializer<IN2> inputDeserializer2 = configuration.getTypeSerializerIn2(userClassLoader);

		int numberOfInputs = configuration.getNumberOfInputs();

		ArrayList<InputGate> inputList1 = new ArrayList<InputGate>();
		ArrayList<InputGate> inputList2 = new ArrayList<InputGate>();

		List<StreamEdge> inEdges = configuration.getInPhysicalEdges(userClassLoader);

		for (int i = 0; i < numberOfInputs; i++) {
			int inputType = inEdges.get(i).getTypeNumber();
			InputGate reader = getEnvironment().getInputGate(i);
			switch (inputType) {
				case 1:
					inputList1.add(reader);
					break;
				case 2:
					inputList2.add(reader);
					break;
				default:
					throw new RuntimeException("Invalid input type number: " + inputType);
			}
		}

		this.inputProcessor = new StreamTwoInputProcessor<>(
				inputList1, inputList2,
				inputDeserializer1, inputDeserializer2,
				this,
				configuration.getCheckpointMode(),
				getCheckpointLock(),
				getEnvironment().getIOManager(),
				getEnvironment().getTaskManagerInfo().getConfiguration(),
				getStreamStatusMaintainer(),
				this.headOperator,
				getEnvironment().getMetricGroup().getIOMetricGroup(),
				input1WatermarkGauge,
				input2WatermarkGauge);

		headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge);
		headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_1_WATERMARK, input1WatermarkGauge);
		headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_2_WATERMARK, input2WatermarkGauge);
		// wrap watermark gauge since registered metrics must be unique
		getEnvironment().getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge::getValue);

		processingTimeHistogram = ((OperatorMetricGroup) headOperator.getMetricGroup())
				.getIOMetricGroup().getTotalProcessingTime();
	}

	@Override
	protected void run() throws Exception {
		// cache processor reference on the stack, to make the code more JIT friendly
		final StreamTwoInputProcessor<IN1, IN2> inputProcessor = this.inputProcessor;

		boolean monitorRegBef = false;
		// all the work happens in the "processInput" method
		while (running) {
			if (!monitorRegBef && getSchedulerMonitor() != null) {
				this.inputProcessor.setSchedulerCollector(schedulerMonitor.getSchedulerCollector());
				monitorRegBef = true;
			}
			long start = System.currentTimeMillis();
			boolean res = inputProcessor.processInput();
			long elapsedTime = System.currentTimeMillis() - start;

			if (!res) {
				return;
			}

			// Update processing time
			processingTimeHistogram.update(elapsedTime);

			// check if this task is scheduled for execution
			if (monitorRegBef && !schedulerMonitor.isScheduled()) {
				// block thread until monitor signal execution
				synchronized (schedulerMonitor) {
					schedulerMonitor.wait();
				}
			}
		}
	}

	@Override
	protected void cleanup() throws Exception {
		if (inputProcessor != null) {
			inputProcessor.cleanup();
		}
	}

	@Override
	protected void cancelTask() {
		running = false;
	}
}
