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

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.watslack.InterEventGenDelayDist;
import org.apache.flink.streaming.api.operators.watslack.NetworkDelayDist;
import org.apache.flink.streaming.api.operators.watslack.SSSampleSizeEstimator;
import org.apache.flink.streaming.api.operators.watslack.SSSizeEstimator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ScheduledFuture;

/**
 * Source contexts for various stream time characteristics.
 */
public class StreamSourceContexts {

	protected static final Logger LOG = LoggerFactory.getLogger(StreamSourceContexts.class);

	/**
	 * Depending on the {@link TimeCharacteristic}, this method will return the adequate
	 * {@link org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext}. That is:
	 * <ul>
	 *     <li>{@link TimeCharacteristic#IngestionTime} = {@code AutomaticWatermarkContext}</li>
	 *     <li>{@link TimeCharacteristic#ProcessingTime} = {@code NonTimestampContext}</li>
	 *     <li>{@link TimeCharacteristic#EventTime} = {@code ManualWatermarkContext}</li>
	 * </ul>
	 * */
	public static <OUT> SourceFunction.SourceContext<OUT> getSourceContext(
			TimeCharacteristic timeCharacteristic,
			ProcessingTimeService processingTimeService,
			Object checkpointLock,
			StreamStatusMaintainer streamStatusMaintainer,
			Output<StreamRecord<OUT>> output,
			long watermarkInterval,
			long idleTimeout) {

		final SourceFunction.SourceContext<OUT> ctx;
		LOG.info("Identifying Source context of TC=%d", timeCharacteristic);
		switch (timeCharacteristic) {
			case EventTime:
				ctx = new WatSlackWatermarkContext<>(
						output,
						processingTimeService,
						new NetworkDelayDist(1024),
						new InterEventGenDelayDist(1024),
						100, // Watermark every 100ms
						checkpointLock,
						streamStatusMaintainer,
						idleTimeout);
				break;
			case IngestionTime:
				ctx = new AutomaticWatermarkContext<>(
					output,
					watermarkInterval,
					processingTimeService,
					checkpointLock,
					streamStatusMaintainer,
					idleTimeout);
				break;
			case ProcessingTime:
				ctx = new NonTimestampContext<>(checkpointLock, output);
				break;
			default:
				throw new IllegalArgumentException(String.valueOf(timeCharacteristic));
		}
		return ctx;
	}

	/**
	 * A source context that attached {@code -1} as a timestamp to all records, and that
	 * does not forward watermarks.
	 */
	private static class NonTimestampContext<T> implements SourceFunction.SourceContext<T> {

		private final Object lock;
		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;

		private NonTimestampContext(Object checkpointLock, Output<StreamRecord<T>> output) {
			this.lock = Preconditions.checkNotNull(checkpointLock, "The checkpoint lock cannot be null.");
			this.output = Preconditions.checkNotNull(output, "The output cannot be null.");
			this.reuse = new StreamRecord<>(null);
		}

		@Override
		public void collect(T element) {
			synchronized (lock) {
				output.collect(reuse.replace(element));
			}
		}

		@Override
		public void collectWithTimestamp(T element, long timestamp) {
			// ignore the timestamp
			collect(element);
		}

		@Override
		public void emitWatermark(Watermark mark) {
			// do nothing
		}

		@Override
		public void markAsTemporarilyIdle() {
			// do nothing
		}

		@Override
		public Object getCheckpointLock() {
			return lock;
		}

		@Override
		public void close() {}
	}

	/**
	 * {@link SourceFunction.SourceContext} to be used for sources with automatic timestamps
	 * and watermark emission.
	 */
	private static class AutomaticWatermarkContext<T> extends WatermarkContext<T> {

		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;

		private final long watermarkInterval;

		private volatile ScheduledFuture<?> nextWatermarkTimer;
		private volatile long nextWatermarkTime;

		private long lastRecordTime;

		private AutomaticWatermarkContext(
				final Output<StreamRecord<T>> output,
				final long watermarkInterval,
				final ProcessingTimeService timeService,
				final Object checkpointLock,
				final StreamStatusMaintainer streamStatusMaintainer,
				final long idleTimeout) {

			super(timeService, checkpointLock, streamStatusMaintainer, idleTimeout);

			this.output = Preconditions.checkNotNull(output, "The output cannot be null.");

			Preconditions.checkArgument(watermarkInterval >= 1L, "The watermark interval cannot be smaller than 1 ms.");
			this.watermarkInterval = watermarkInterval;

			this.reuse = new StreamRecord<>(null);

			this.lastRecordTime = Long.MIN_VALUE;

			long now = this.timeService.getCurrentProcessingTime();
			this.nextWatermarkTimer = this.timeService.registerTimer(now + watermarkInterval,
				new WatermarkEmittingTask(this.timeService, checkpointLock, output));
		}

		@Override
		protected void processAndCollect(T element) {
			lastRecordTime = this.timeService.getCurrentProcessingTime();
			output.collect(reuse.replace(element, lastRecordTime));

			// this is to avoid lock contention in the lockingObject by
			// sending the watermark before the firing of the watermark
			// emission task.
			if (lastRecordTime > nextWatermarkTime) {
				// in case we jumped some watermarks, recompute the next watermark time
				final long watermarkTime = lastRecordTime - (lastRecordTime % watermarkInterval);
				nextWatermarkTime = watermarkTime + watermarkInterval;
				output.emitWatermark(new Watermark(watermarkTime));

				// we do not need to register another timer here
				// because the emitting task will do so.
			}
		}

		@Override
		protected void processAndCollectWithTimestamp(T element, long timestamp) {
			processAndCollect(element);
		}

		@Override
		protected boolean allowWatermark(Watermark mark) {
			// allow Long.MAX_VALUE since this is the special end-watermark that for example the Kafka source emits
			return mark.getTimestamp() == Long.MAX_VALUE && nextWatermarkTime != Long.MAX_VALUE;
		}

		/** This will only be called if allowWatermark returned {@code true}. */
		@Override
		protected void processAndEmitWatermark(Watermark mark) {
			nextWatermarkTime = Long.MAX_VALUE;
			output.emitWatermark(mark);

			// we can shutdown the watermark timer now, no watermarks will be needed any more.
			// Note that this procedure actually doesn't need to be synchronized with the lock,
			// but since it's only a one-time thing, doesn't hurt either
			final ScheduledFuture<?> nextWatermarkTimer = this.nextWatermarkTimer;
			if (nextWatermarkTimer != null) {
				nextWatermarkTimer.cancel(true);
			}
		}

		@Override
		public void close() {
			super.close();

			final ScheduledFuture<?> nextWatermarkTimer = this.nextWatermarkTimer;
			if (nextWatermarkTimer != null) {
				nextWatermarkTimer.cancel(true);
			}
		}

		private class WatermarkEmittingTask implements ProcessingTimeCallback {

			private final ProcessingTimeService timeService;
			private final Object lock;
			private final Output<StreamRecord<T>> output;

			private WatermarkEmittingTask(
					ProcessingTimeService timeService,
					Object checkpointLock,
					Output<StreamRecord<T>> output) {
				this.timeService = timeService;
				this.lock = checkpointLock;
				this.output = output;
			}

			@Override
			public void onProcessingTime(long timestamp) {
				final long currentTime = timeService.getCurrentProcessingTime();

				synchronized (lock) {
					// we should continue to automatically emit watermarks if we are active
					if (streamStatusMaintainer.getStreamStatus().isActive()) {
						if (idleTimeout != -1 && currentTime - lastRecordTime > idleTimeout) {
							// if we are configured to detect idleness, piggy-back the idle detection check on the
							// watermark interval, so that we may possibly discover idle sources faster before waiting
							// for the next idle check to fire
							markAsTemporarilyIdle();

							// no need to finish the next check, as we are now idle.
							cancelNextIdleDetectionTask();
						} else if (currentTime > nextWatermarkTime) {
							// align the watermarks across all machines. this will ensure that we
							// don't have watermarks that creep along at different intervals because
							// the machine clocks are out of sync
							final long watermarkTime = currentTime - (currentTime % watermarkInterval);

							output.emitWatermark(new Watermark(watermarkTime));
							nextWatermarkTime = watermarkTime + watermarkInterval;
						}
					}
				}

				long nextWatermark = currentTime + watermarkInterval;
				nextWatermarkTimer = this.timeService.registerTimer(
						nextWatermark, new WatermarkEmittingTask(this.timeService, lock, output));
			}
		}
	}

	/**
	 * {@link SourceFunction.SourceContext} to be used for sources with adaptive delays for
	 * watermark emission.
	 */
	private static class WatSlackWatermarkContext<T> extends WatermarkContext<T> {

        private final Output<StreamRecord<T>> output;
        private final StreamRecord<T> reuse;
        /* WatSlack Distributions & Estimators */
        private final NetworkDelayDist streamNetDelay;
        private final InterEventGenDelayDist streamGenDelay;
        private final SSSizeEstimator ssSizeEstimator;
        private final SSSampleSizeEstimator ssSampleSizeEstimator;

		private final long watermarkFrequency;

		/* WatSlack Specific Data Structures */
		private Set<Long> observedSSSet;
		private Map<Long, Long> eventsProcPerSSMap;

		/* Utils */
		private final Random random;

        private WatSlackWatermarkContext(
                final Output<StreamRecord<T>> output,
                final ProcessingTimeService timeService,
                final NetworkDelayDist streamNetDelay,
                final InterEventGenDelayDist streamGenDelay,
                final long watermarkFrequency,
                final Object checkpointLock,
                final StreamStatusMaintainer streamStatusMaintainer,
                final long idleTimeout) {
            super(timeService, checkpointLock, streamStatusMaintainer, idleTimeout);
            LOG.info("WatSlack", "Running StreamSourceContexts");

            this.output = Preconditions.checkNotNull(output, "The output cannot be null.");
			this.reuse = new StreamRecord<>(null);

            /* WatSlack Estimators */
            this.streamNetDelay = Preconditions.checkNotNull(streamNetDelay, "The NetworkDelayDist cannot be null.");
			this.streamGenDelay = Preconditions.checkNotNull(streamGenDelay, "The InterEventGenDelayDist cannot be null.");
			this.watermarkFrequency = watermarkFrequency;
			this.ssSizeEstimator =
					new SSSizeEstimator(this.streamNetDelay, this.streamGenDelay, this.watermarkFrequency);
            this.ssSampleSizeEstimator = new SSSampleSizeEstimator(this.ssSizeEstimator);
			/* Watermark specific Data Structures */
			this.observedSSSet = new HashSet<>();
			this.eventsProcPerSSMap = new HashMap<>();

			this.random = new Random();
        }

        @Override
        protected void processAndCollect(T element) {
            throw new UnsupportedOperationException("Use processAndCollectWithTimestamp");
        }

        @Override
        protected void processAndCollectWithTimestamp(T element, long timestamp) {
        	/*
        	 * The algorithm is divided into 3 steps:
        	 * 1) Collect timestamp
        	 * 2) Sampling algorithm
        	 * 3) Watermark Emission
        	 */
			long ssIndex = (long) Math.ceil(timestamp / (watermarkFrequency * 1.0));
			if (!observedSSSet.contains(ssIndex)) {
				observedSSSet.add(ssIndex);
				eventsProcPerSSMap.put(ssIndex, 0L);
			}

			// 1) Collect event timestamp.
			streamNetDelay.add(ssIndex, this.timeService.getCurrentProcessingTime() - timestamp);
			streamGenDelay.add(ssIndex, timestamp);

			// 2) Sampling algorithm.
			long processedPerSS = eventsProcPerSSMap.get(ssIndex);
			// Let us do it with rate = 0.9 for now.
			if (random.nextDouble() <= 0.9) {
				output.collect(reuse.replace(element, timestamp));
				eventsProcPerSSMap.put(ssIndex, ++processedPerSS);
			} else {
				LOG.info("Dropped a tuple");
			}

			/*
			 * 3) Watermark Emission
			 *
			 * Watermarks are emitted in one of two cases:
			 * 	a) θ events were collected; or
			 * 	b) time expired.
			 */

			// Check a)
			long targetSampleSize = ssSampleSizeEstimator.estimate(ssIndex);
			if (processedPerSS >= targetSampleSize) {
			    // Substream is done!
                streamNetDelay.finalize(ssIndex);
                output.emitWatermark(new Watermark(ssIndex * (watermarkFrequency + 1)));

			}
			LOG.info("Watermark algorithm here!");
        }

        @Override
        protected boolean allowWatermark(Watermark mark) {
            return mark.getTimestamp() == Long.MAX_VALUE;
        }

        /**
         * This will only be called if allowWatermark returned {@code true}.
         */
        @Override
        protected void processAndEmitWatermark(Watermark mark) {
            output.emitWatermark(mark);
        }

        @Override
        public void close() {
            super.close();
        }
    }

	/**
	 * A SourceContext for event time. Sources may directly attach timestamps and generate
	 * watermarks, but if records are emitted without timestamps, no timestamps are automatically
	 * generated and attached. The records will simply have no timestamp in that case.
	 *
	 * <p>Streaming topologies can use timestamp assigner functions to override the timestamps
	 * assigned here.
	 */
	private static class ManualWatermarkContext<T> extends WatermarkContext<T> {

		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;

		private ManualWatermarkContext(
				final Output<StreamRecord<T>> output,
				final ProcessingTimeService timeService,
				final Object checkpointLock,
				final StreamStatusMaintainer streamStatusMaintainer,
				final long idleTimeout) {

			super(timeService, checkpointLock, streamStatusMaintainer, idleTimeout);

			this.output = Preconditions.checkNotNull(output, "The output cannot be null.");
			this.reuse = new StreamRecord<>(null);
		}

		@Override
		protected void processAndCollect(T element) {
			output.collect(reuse.replace(element));
		}

		@Override
		protected void processAndCollectWithTimestamp(T element, long timestamp) {
			output.collect(reuse.replace(element, timestamp));
		}

		@Override
		protected void processAndEmitWatermark(Watermark mark) {
			output.emitWatermark(mark);
		}

		@Override
		protected boolean allowWatermark(Watermark mark) {
			return true;
		}
	}

	/**
	 * An abstract {@link SourceFunction.SourceContext} that should be used as the base for
	 * stream source contexts that are relevant with {@link Watermark}s.
	 *
	 * <p>Stream source contexts that are relevant with watermarks are responsible of manipulating
	 * the current {@link StreamStatus}, so that stream status can be correctly propagated
	 * downstream. Please refer to the class-level documentation of {@link StreamStatus} for
	 * information on how stream status affects watermark advancement at downstream tasks.
	 *
	 * <p>This class implements the logic of idleness detection. It fires idleness detection
	 * tasks at a given interval; if no records or watermarks were collected by the source context
	 * between 2 consecutive checks, it determines the source to be IDLE and correspondingly
	 * toggles the status. ACTIVE status resumes as soon as some record or watermark is collected
	 * again.
	 */
	private abstract static class WatermarkContext<T> implements SourceFunction.SourceContext<T> {

		protected final ProcessingTimeService timeService;
		protected final Object checkpointLock;
		protected final StreamStatusMaintainer streamStatusMaintainer;
		protected final long idleTimeout;

		private ScheduledFuture<?> nextCheck;

		/**
		 * This flag will be reset to {@code true} every time the next check is scheduled.
		 * Whenever a record or watermark is collected, the flag will be set to {@code false}.
		 *
		 * <p>When the scheduled check is fired, if the flag remains to be {@code true}, the check
		 * will fail, and our current status will determined to be IDLE.
		 */
		private volatile boolean failOnNextCheck;

		/**
		 * Create a watermark context.
		 *
		 * @param timeService the time service to schedule idleness detection tasks
		 * @param checkpointLock the checkpoint lock
		 * @param streamStatusMaintainer the stream status maintainer to toggle and retrieve current status
		 * @param idleTimeout (-1 if idleness checking is disabled)
		 */
		public WatermarkContext(
				final ProcessingTimeService timeService,
				final Object checkpointLock,
				final StreamStatusMaintainer streamStatusMaintainer,
				final long idleTimeout) {

			this.timeService = Preconditions.checkNotNull(timeService, "Time Service cannot be null.");
			this.checkpointLock = Preconditions.checkNotNull(checkpointLock, "Checkpoint Lock cannot be null.");
			this.streamStatusMaintainer = Preconditions.checkNotNull(streamStatusMaintainer, "Stream Status Maintainer cannot be null.");

			if (idleTimeout != -1) {
				Preconditions.checkArgument(idleTimeout >= 1, "The idle timeout cannot be smaller than 1 ms.");
			}
			this.idleTimeout = idleTimeout;

			scheduleNextIdleDetectionTask();
		}

		@Override
		public void collect(T element) {
			synchronized (checkpointLock) {
				streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);

				if (nextCheck != null) {
					this.failOnNextCheck = false;
				} else {
					scheduleNextIdleDetectionTask();
				}

				processAndCollect(element);
			}
		}

		@Override
		public void collectWithTimestamp(T element, long timestamp) {
			synchronized (checkpointLock) {
				streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);

				if (nextCheck != null) {
					this.failOnNextCheck = false;
				} else {
					scheduleNextIdleDetectionTask();
				}

				processAndCollectWithTimestamp(element, timestamp);
			}
		}

		@Override
		public void emitWatermark(Watermark mark) {
			if (allowWatermark(mark)) {
				synchronized (checkpointLock) {
					streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);

					if (nextCheck != null) {
						this.failOnNextCheck = false;
					} else {
						scheduleNextIdleDetectionTask();
					}

					processAndEmitWatermark(mark);
				}
			}
		}

		@Override
		public void markAsTemporarilyIdle() {
			synchronized (checkpointLock) {
				streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
			}
		}

		@Override
		public Object getCheckpointLock() {
			return checkpointLock;
		}

		@Override
		public void close() {
			cancelNextIdleDetectionTask();
		}

		private class IdlenessDetectionTask implements ProcessingTimeCallback {
			@Override
			public void onProcessingTime(long timestamp) throws Exception {
				synchronized (checkpointLock) {
					// set this to null now;
					// the next idleness detection will be scheduled again
					// depending on the below failOnNextCheck condition
					nextCheck = null;

					if (failOnNextCheck) {
						markAsTemporarilyIdle();
					} else {
						scheduleNextIdleDetectionTask();
					}
				}
			}
		}

		private void scheduleNextIdleDetectionTask() {
			if (idleTimeout != -1) {
				// reset flag; if it remains true when task fires, we have detected idleness
				failOnNextCheck = true;
				nextCheck = this.timeService.registerTimer(
					this.timeService.getCurrentProcessingTime() + idleTimeout,
					new IdlenessDetectionTask());
			}
		}

		protected void cancelNextIdleDetectionTask() {
			final ScheduledFuture<?> nextCheck = this.nextCheck;
			if (nextCheck != null) {
				nextCheck.cancel(true);
			}
		}

		// ------------------------------------------------------------------------
		//	Abstract methods for concrete subclasses to implement.
		//  These methods are guaranteed to be synchronized on the checkpoint lock,
		//  so implementations don't need to do so.
		// ------------------------------------------------------------------------

		/** Process and collect record. */
		protected abstract void processAndCollect(T element);

		/** Process and collect record with timestamp. */
		protected abstract void processAndCollectWithTimestamp(T element, long timestamp);

		/** Whether or not a watermark should be allowed. */
		protected abstract boolean allowWatermark(Watermark mark);

		/**
		 * Process and emit watermark. Only called if
		 * {@link WatermarkContext#allowWatermark(Watermark)} returns {@code true}.
		 */
		protected abstract void processAndEmitWatermark(Watermark mark);

	}
}
