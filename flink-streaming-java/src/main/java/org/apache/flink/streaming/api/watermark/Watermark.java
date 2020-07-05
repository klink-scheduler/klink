/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.watermark;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

/**
 * A Watermark tells operators that no elements with a timestamp older or equal
 * to the watermark timestamp should arrive at the operator. Watermarks are emitted at the
 * sources and propagate through the operators of the topology. Operators must themselves emit
 * watermarks to downstream operators using
 * {@link org.apache.flink.streaming.api.operators.Output#emitWatermark(Watermark)}. Operators that
 * do not internally buffer elements can always forward the watermark that they receive. Operators
 * that buffer elements, such as window operators, must forward a watermark after emission of
 * elements that is triggered by the arriving watermark.
 *
 * <p>In some cases a watermark is only a heuristic and operators should be able to deal with
 * late elements. They can either discard those or update the result and emit updates/retractions
 * to downstream operations.
 *
 * <p>When a source closes it will emit a final watermark with timestamp {@code Long.MAX_VALUE}.
 * When an operator receives this it will know that no more input will be arriving in the future.
 */
@PublicEvolving
public final class Watermark extends StreamElement {

	/** The watermark that signifies end-of-event-time. */
	public static final Watermark MAX_WATERMARK = new Watermark(Long.MAX_VALUE);

	// ------------------------------------------------------------------------

	/** The timestamp of the watermark in milliseconds. */
	private final long timestamp;

	/** The timestamp of ingestion, used to measure watermark propagation delay. */
	private final long ingestionTimestamp;

	/** Boolean flag indicating if the watermark was emitting, used to measure watermark propagation delay. */
	private boolean isEmitting;

	/**
	 * Creates a new watermark with the given timestamp in milliseconds.
	 */
	public Watermark(long timestamp) {
		this.timestamp = timestamp;
		this.ingestionTimestamp = System.currentTimeMillis();
		this.isEmitting = false;
	}

	public Watermark(long timestamp, long ingestionTimestamp, boolean isEmitting) {
		this.timestamp = timestamp;
		this.ingestionTimestamp = ingestionTimestamp;
		this.isEmitting = isEmitting;
	}
	/**
	 * Returns the timestamp associated with this {@link Watermark} in milliseconds.
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * Returns the ingestion timestamp associated with this {@link Watermark} in milliseconds.
	 */
	public long getIngestionTimestamp() {
		return ingestionTimestamp;
	}

	/**
	 * Sets the timestamp associated with this {@link Watermark} in milliseconds.
	 */
	public void setIsEmitting() {
		this.isEmitting = true;
	}

	/**
	 * Returns the emitting flag associated with this {@link Watermark}.
	 */
	public boolean isEmitting() {
		return isEmitting;
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		Watermark that = (Watermark) o;
		return this == that || (that != null
				&& that.getClass() == Watermark.class
				&& that.timestamp == this.timestamp
				&& that.ingestionTimestamp == this.ingestionTimestamp)
                && that.isEmitting == this.isEmitting;
	}

	@Override
	public int hashCode() {
		int result = (int) (timestamp ^ (timestamp >>> 32));
		result = 31 * result + (int) (ingestionTimestamp ^ (ingestionTimestamp >>> 32));
		result = 31 * result + (isEmitting ? 1 : 0);
		return result;
	}

	@Override
	public String toString() {
		return "Watermark{" +
				"timestamp=" + timestamp +
				", ingestionTimestamp=" + ingestionTimestamp +
				", isEmitting=" + isEmitting +
				'}';
	}
}
