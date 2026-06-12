/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

/** Replicates a set of topic-partitions. */
public class MirrorSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceTask.class);

    private KafkaConsumer<byte[], byte[]> consumer;
    private String sourceClusterAlias;
    private Duration pollTimeout;
    private ReplicationPolicy replicationPolicy;
    private MirrorSourceMetrics metrics;
    private boolean stopping = false;
    private Semaphore consumerAccess;
    private OffsetSyncWriter offsetSyncWriter;
    private final Map<TopicPartition, Long> expectedOffsets = new java.util.HashMap<>();
    private final Set<String> compactedTopics = new java.util.HashSet<>();

    public MirrorSourceTask() {}

    // for testing
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter) {
        this.consumer = consumer;
        this.metrics = metrics;
        this.sourceClusterAlias = sourceClusterAlias;
        this.replicationPolicy = replicationPolicy;
        consumerAccess = new Semaphore(1);
        this.offsetSyncWriter = offsetSyncWriter;
    }

    @Override
    public void start(Map<String, String> props) {
        MirrorSourceTaskConfig config = new MirrorSourceTaskConfig(props);
        consumerAccess = new Semaphore(1);  // let one thread at a time access the consumer
        sourceClusterAlias = config.sourceClusterAlias();
        metrics = config.metrics();
        pollTimeout = config.consumerPollTimeout();
        replicationPolicy = config.replicationPolicy();
        if (config.emitOffsetSyncsEnabled()) {
            offsetSyncWriter = new OffsetSyncWriter(config);
        }

        // Disable automatic offset reset (set to 'none') so that any OffsetOutOfRangeException
        // is surfaced to the task level and can be explicitly handled for data loss detection.
        Map<String, Object> consumerProps = config.sourceConsumerConfig("replication-consumer");
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        consumer = MirrorUtils.newConsumer(consumerProps);
        Set<TopicPartition> taskTopicPartitions = config.taskTopicPartitions();
        initializeConsumer(taskTopicPartitions);

        log.info("{} replicating {} topic-partitions {}->{}: {}.", Thread.currentThread().getName(),
            taskTopicPartitions.size(), sourceClusterAlias, config.targetClusterAlias(), taskTopicPartitions);
    }

    @Override
    public void commit() {
        // Handle delayed and pending offset syncs only when offsetSyncWriter is available
        if (offsetSyncWriter != null) {
            // Offset syncs which were not emitted immediately due to their offset spacing should be sent periodically
            // This ensures that low-volume topics aren't left with persistent lag at the end of the topic
            offsetSyncWriter.promoteDelayedOffsetSyncs();
            // Publish any offset syncs that we've queued up, but have not yet been able to publish
            // (likely because we previously reached our limit for number of outstanding syncs)
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }

    @Override
    public void stop() {
        long start = System.currentTimeMillis();
        stopping = true;
        consumer.wakeup();
        try {
            consumerAccess.acquire();
        } catch (InterruptedException e) {
            log.warn("Interrupted waiting for access to consumer. Will try closing anyway."); 
        }
        Utils.closeQuietly(consumer, "source consumer");
        Utils.closeQuietly(offsetSyncWriter, "offset sync writer");
        Utils.closeQuietly(metrics, "metrics");
        log.info("Stopping {} took {} ms.", Thread.currentThread().getName(), System.currentTimeMillis() - start);
    }
  
    @Override
    public String version() {
        return new MirrorSourceConnector().version();
    }

    /**
     * Verifies that the offset sequence of records polled from the source topic is continuous.
     * Detects potential log truncation (offset gaps) or topic recreation/resets (backward offsets).
     *
     * @param tp the topic partition of the record
     * @param actualOffset the offset of the polled record
     */
    private void verifyOffsetSequence(TopicPartition tp, long actualOffset) {
        Long expectedOffset = expectedOffsets.get(tp);

        log.debug("Checking offset sequence: partition={}, actualOffset={}, expectedOffset={}", tp, actualOffset,
                expectedOffset);

        if (expectedOffset == null) {
            return;
        }

        // If the polled offset is greater than expected, it indicates an offset gap.
        // This is normal for compacted topics where duplicate keys are pruned, but
        // indicates data truncation (loss) on regular topics.
        if (actualOffset > expectedOffset) {
            if (compactedTopics.contains(tp.topic())) {
                log.debug("Offset gap on compacted topic {} (expected={}, got={}) -- advancing pointer.", tp,
                        expectedOffset, actualOffset);
                expectedOffsets.put(tp, actualOffset + 1L);
                return;
            }
            log.error("[CRITICAL REPLICATION GAP] Log truncation detected on partition {}. "
                    + "Expected offset: {}, but received offset: {}. {} messages lost! "
                    + "Terminating MirrorMaker process immediately (fail-fast).",
                    tp, expectedOffset, actualOffset, (actualOffset - expectedOffset));
            exitOrThrow("FAIL-FAST: Log truncation gap on " + tp, null);
            return;
        }

        // If the polled offset is less than expected, the topic has likely been deleted and recreated,
        // causing offsets to reset back to 0. In this case, seek the consumer to the beginning
        // so that the next poll starts cleanly, and accept the current record as valid post-reset data.
        if (actualOffset < expectedOffset) {
            log.warn(
                    "[TOPIC RESET DETECTED] Source topic-partition {} reset at {}. Previous expected offset: {}, new offset: {}. Automatically re-subscribing from beginning.",
                    tp, Instant.now(), expectedOffset, actualOffset);
            consumer.seekToBeginning(Collections.singletonList(tp));
            expectedOffsets.put(tp, actualOffset + 1L);
        }
    }

    /**
     * Inspects OffsetOutOfRangeException boundaries to determine if the consumer is out of bounds
     * due to data retention purging (critical data loss) or topic recreation (reset).
     *
     * @param e the out-of-range exception thrown by the Kafka consumer
     */
    private void handleExceptionBounds(OffsetOutOfRangeException e) {
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(e.offsetOutOfRangePartitions().keySet());

        for (TopicPartition tp : e.offsetOutOfRangePartitions().keySet()) {
            Long expected = expectedOffsets.get(tp);
            Long beginning = beginningOffsets.get(tp);

            if (expected == null || beginning == null) {
                continue;
            }

            // Expected offset is before the earliest available offset on the broker.
            // This indicates unrecoverable data loss unless the topic is compacted.
            if (expected < beginning) {
                if (compactedTopics.contains(tp.topic())) {
                    continue;
                }
                log.error("[CRITICAL DATA LOSS] Partition {} has purged records. "
                        + "Expected offset: {}, Earliest available offset: {}. "
                        + "Terminating MirrorMaker process immediately (fail-fast).",
                        tp, expected, beginning);
                exitOrThrow("Data loss at startup on partition " + tp, null);
                continue;
            }

            // Earliest available offset is 0 but we expected a higher offset,
            // indicating the topic was deleted and recreated.
            if (beginning == 0 && expected > 0) {
                log.warn(
                        "[TOPIC RESET DETECTED] Topic appears recreated: {} at {}. Was at offset {}, resetting to beginning.",
                        tp, Instant.now(), expected);
                consumer.seekToBeginning(Collections.singletonList(tp));
                expectedOffsets.put(tp, 0L);
            }
        }
    }

    @Override
    public List<SourceRecord> poll() {
        if (!consumerAccess.tryAcquire()) {
            return null;
        }
        if (stopping) {
            return null;
        }
        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                long actualOffset = record.offset();

                // Validate offset continuity and handle topic reset/truncation scenarios.
                verifyOffsetSequence(tp, actualOffset);

                // Update expected offset to the next one in sequence.
                expectedOffsets.put(tp, actualOffset + 1L);

                SourceRecord converted = convertRecord(record);
                sourceRecords.add(converted);
                TopicPartition targetTopicPartition = new TopicPartition(converted.topic(), converted.kafkaPartition());
                metrics.recordAge(targetTopicPartition, System.currentTimeMillis() - record.timestamp());
                metrics.recordBytes(targetTopicPartition, byteSize(record.value()));
            }
            if (sourceRecords.isEmpty()) {
                // WorkerSourceTasks expects non-zero batch size
                return null;
            } else {
                log.trace("Polled {} records from {}.", sourceRecords.size(), records.partitions());
                return sourceRecords;
            }
        } catch (OffsetOutOfRangeException e) {
            log.error("[OUT OF RANGE EXCEPTION] Caught out-of-bounds offset sequence from consumer driver level.");
            handleExceptionBounds(e);
            return null;
        } catch (WakeupException e) {
            return null;
        } catch (KafkaException e) {
            log.warn("Failure during poll.", e);
            return null;
        } catch (Throwable e)  {
            log.error("Failure during poll.", e);
            // allow Connect to deal with the exception
            throw e;
        } finally {
            consumerAccess.release();
        }
    }
 
    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        if (stopping) {
            return;
        }
        if (metadata == null) {
            log.debug("No RecordMetadata (source record was probably filtered out during transformation) -- can't sync offsets for {}.", record.topic());
            return;
        }
        if (!metadata.hasOffset()) {
            log.error("RecordMetadata has no offset -- can't sync offsets for {}.", record.topic());
            return;
        }
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        long latency = System.currentTimeMillis() - record.timestamp();
        metrics.countRecord(topicPartition);
        metrics.replicationLatency(topicPartition, latency);
        // Queue offset syncs only when offsetWriter is available
        if (offsetSyncWriter != null) {
            TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(record.sourcePartition());
            long upstreamOffset = MirrorUtils.unwrapOffset(record.sourceOffset());
            long downstreamOffset = metadata.offset();
            offsetSyncWriter.maybeQueueOffsetSyncs(sourceTopicPartition, upstreamOffset, downstreamOffset);
            // We may be able to immediately publish an offset sync that we've queued up here
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }
 
    private Map<TopicPartition, Long> loadOffsets(Set<TopicPartition> topicPartitions) {
        return topicPartitions.stream().collect(Collectors.toMap(x -> x, this::loadOffset));
    }

    private Long loadOffset(TopicPartition topicPartition) {
        Map<String, Object> wrappedPartition = MirrorUtils.wrapPartition(topicPartition, sourceClusterAlias);
        Map<String, Object> wrappedOffset = context.offsetStorageReader().offset(wrappedPartition);
        return MirrorUtils.unwrapOffset(wrappedOffset);
    }

    // visible for testing
    void initializeConsumer(Set<TopicPartition> taskTopicPartitions) {
        Map<TopicPartition, Long> topicPartitionOffsets = loadOffsets(taskTopicPartitions);
        consumer.assign(topicPartitionOffsets.keySet());
        long uncommittedCount = topicPartitionOffsets.values().stream().filter(this::isUncommitted).count();
        log.info("Starting with {} previously uncommitted partitions.", uncommittedCount);
        Set<TopicPartition> committedPartitions = topicPartitionOffsets.entrySet().stream()
                .filter(e -> !isUncommitted(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        // Batch query beginning and end offsets across all partitions to minimize broker requests.
        // Beginning offsets are used for startup data loss validation, whereas end offsets are
        // used to identify topic resets (i.e. recreated topics).
        Map<TopicPartition, Long> allBeginningOffsets = consumer.beginningOffsets(topicPartitionOffsets.keySet());
        Map<TopicPartition, Long> allEndOffsets = consumer.endOffsets(topicPartitionOffsets.keySet());

        // Partitions identified as recreated/reset at startup.
        Set<TopicPartition> resetPartitions = new java.util.HashSet<>();

        if (!committedPartitions.isEmpty()) {
            for (TopicPartition tp : committedPartitions) {
                long earliestAvailable = allBeginningOffsets.getOrDefault(tp, 0L);
                long lastCommitted = topicPartitionOffsets.get(tp);
                long endOffset = allEndOffsets.getOrDefault(tp, 0L);

                // Heuristic topic reset detection on startup:
                // We assume a partition has been reset/recreated if:
                // (a) the broker's earliest available offset is 0,
                // (b) MirrorMaker has a previously committed/stored offset greater than 0,
                // (c) the previously committed offset exceeds the current log end offset on the broker.
                // If this condition is met, we skip data-loss alerts and resubscribe starting from 0.
                if (earliestAvailable == 0 && lastCommitted > 0 && lastCommitted > endOffset) {
                    log.warn("[TOPIC RESET DETECTED] Source topic-partition {} reset at {}. "
                            + "Previous committed offset: {}, current log end offset: {}. "
                            + "Automatically re-subscribing from beginning.",
                            tp, Instant.now(), lastCommitted, endOffset);
                    resetPartitions.add(tp);
                    continue; // Skip data-loss validation; seek is handled below.
                }

                // Verify offset bounds to detect potential data loss:
                // If MirrorMaker's stored offset is behind the broker's earliest available offset,
                // records have been purged (e.g., due to log retention) before they could be replicated.
                // Except for compacted topics, this indicates unrecoverable data loss.
                if (earliestAvailable > (lastCommitted + 1)) {
                    if (compactedTopics.contains(tp.topic())) {
                        continue;
                    }
                    log.error("[CRITICAL DATA LOSS AT STARTUP] Partition {} has purged records. "
                            + "Last committed offset: {}, Earliest available offset: {}. "
                            + "Terminating MirrorMaker process immediately (fail-fast).",
                            tp, lastCommitted, earliestAvailable);
                    exitOrThrow("Data loss at startup on partition " + tp, null);
                }
            }
        }

        // Seek reset partitions to the beginning as a batch before the per-partition loop.
        if (!resetPartitions.isEmpty()) {
            consumer.seekToBeginning(resetPartitions);
        }

        // SEEK TO CORRECT STARTING OFFSETS
        topicPartitionOffsets.forEach((topicPartition, offset) -> {
            // Do not call seek on partitions that don't have an existing offset committed.
            long nextOffset;
            if (resetPartitions.contains(topicPartition)) {
                nextOffset = 0L;
            } else if (isUncommitted(offset)) {
                nextOffset = 0L;
            } else {
                nextOffset = offset + 1L;
            }
            expectedOffsets.put(topicPartition, nextOffset);
            log.debug("Initialized expected offset for {} -> {} (log beginning: {})",
                    topicPartition, nextOffset, allBeginningOffsets.getOrDefault(topicPartition, -1L));
            consumer.seek(topicPartition, nextOffset);
        });
    }

    // visible for testing 
    SourceRecord convertRecord(ConsumerRecord<byte[], byte[]> record) {
        String targetTopic = formatRemoteTopic(record.topic());
        Headers headers = convertHeaders(record);
        return new SourceRecord(
                MirrorUtils.wrapPartition(new TopicPartition(record.topic(), record.partition()), sourceClusterAlias),
                MirrorUtils.wrapOffset(record.offset()),
                targetTopic, record.partition(),
                Schema.OPTIONAL_BYTES_SCHEMA, record.key(),
                Schema.BYTES_SCHEMA, record.value(),
                record.timestamp(), headers);
    }

    private Headers convertHeaders(ConsumerRecord<byte[], byte[]> record) {
        ConnectHeaders headers = new ConnectHeaders();
        for (Header header : record.headers()) {
            headers.addBytes(header.key(), header.value());
        }
        return headers;
    }

    private String formatRemoteTopic(String topic) {
        return replicationPolicy.formatRemoteTopic(sourceClusterAlias, topic);
    }

    private static int byteSize(byte[] bytes) {
        if (bytes == null) {
            return 0;
        } else {
            return bytes.length;
        }
    }

    private boolean isUncommitted(Long offset) {
        return offset == null || offset < 0;
    }

    void putExpectedOffset(TopicPartition tp, long offset) {
        expectedOffsets.put(tp, offset);
    }

    void markTopicAsCompacted(String topic) {
        compactedTopics.add(topic);
    }

    private boolean shouldExitOnDataLoss() {
        // By default, we terminate the JVM to support standalone containers and simple test runners
        // that monitor container processes. In distributed Connect environments, this can be
        // disabled by setting MM2_EXIT_ON_DATA_LOSS=false or -Dmirror.exit.on.data.loss=false.
        String env = System.getenv("MM2_EXIT_ON_DATA_LOSS");
        if (env != null) {
            return !env.equalsIgnoreCase("false");
        }
        return !"false".equalsIgnoreCase(System.getProperty("mirror.exit.on.data.loss"));
    }

    private void exitOrThrow(String message, Throwable cause) {
        if (shouldExitOnDataLoss()) {
            log.error("Exit-on-data-loss is enabled. Terminating JVM.");
            Exit.exit(1);
        }
        if (cause != null) {
            throw new DataLossException(message, cause);
        } else {
            throw new DataLossException(message);
        }
    }

    public static class DataLossException extends ConnectException {
        public DataLossException(String message) {
            super(message);
        }
        public DataLossException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
