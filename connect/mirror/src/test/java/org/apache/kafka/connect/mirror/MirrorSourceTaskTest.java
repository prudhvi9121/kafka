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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.mirror.OffsetSyncWriter.PartitionState;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.connect.mirror.MirrorSourceTask.DataLossException;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import java.util.Collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
// import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MirrorSourceTaskTest {

    @BeforeEach
    public void setUp() {
        Exit.setExitProcedure((statusCode, message) -> {
        });
    }

    @AfterEach
    public void tearDown() {
        Exit.resetExitProcedure();
    }

    @Test
    public void testSerde() {
        byte[] key = new byte[]{'a', 'b', 'c', 'd', 'e'};
        byte[] value = new byte[]{'f', 'g', 'h', 'i', 'j', 'k'};
        Headers headers = new RecordHeaders();
        headers.add("header1", new byte[]{'l', 'm', 'n', 'o'});
        headers.add("header2", new byte[]{'p', 'q', 'r', 's', 't'});
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("topic1", 2, 3L, 4L,
            TimestampType.CREATE_TIME, 5, 6, key, value, headers, Optional.empty());
        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(null, null, "cluster7",
                new DefaultReplicationPolicy(), null);
        SourceRecord sourceRecord = mirrorSourceTask.convertRecord(consumerRecord);
        assertEquals("cluster7.topic1", sourceRecord.topic(),
                "Failure on cluster7.topic1 consumerRecord serde");
        assertEquals(2, sourceRecord.kafkaPartition().intValue(),
                "sourceRecord kafka partition is incorrect");
        assertEquals(new TopicPartition("topic1", 2), MirrorUtils.unwrapPartition(sourceRecord.sourcePartition()),
                "topic1 unwrapped from sourcePartition is incorrect");
        assertEquals(3L, MirrorUtils.unwrapOffset(sourceRecord.sourceOffset()).longValue(),
                "sourceRecord's sourceOffset is incorrect");
        assertEquals(4L, sourceRecord.timestamp().longValue(),
                "sourceRecord's timestamp is incorrect");
        assertEquals(key, sourceRecord.key(), "sourceRecord's key is incorrect");
        assertEquals(value, sourceRecord.value(), "sourceRecord's value is incorrect");
        assertEquals(headers.lastHeader("header1").value(), sourceRecord.headers().lastWithName("header1").value(),
                "sourceRecord's header1 is incorrect");
        assertEquals(headers.lastHeader("header2").value(), sourceRecord.headers().lastWithName("header2").value(),
                "sourceRecord's header2 is incorrect");
    }

    @Test
    public void testOffsetSync() {
        OffsetSyncWriter.PartitionState partitionState = new OffsetSyncWriter.PartitionState(50);

        assertTrue(partitionState.update(0, 100), "always emit offset sync on first update");
        assertTrue(partitionState.shouldSyncOffsets, "should sync offsets");
        partitionState.reset();
        assertFalse(partitionState.shouldSyncOffsets, "should sync offsets to false");
        assertTrue(partitionState.update(2, 102), "upstream offset skipped -> resync");
        partitionState.reset();
        assertFalse(partitionState.update(3, 152), "no sync");
        partitionState.reset();
        assertTrue(partitionState.update(4, 153), "one past target offset");
        partitionState.reset();
        assertFalse(partitionState.update(5, 154), "no sync");
        partitionState.reset();
        assertFalse(partitionState.update(6, 203), "no sync");
        partitionState.reset();
        assertTrue(partitionState.update(7, 204), "one past target offset");
        partitionState.reset();
        assertTrue(partitionState.update(2, 206), "upstream reset");
        partitionState.reset();
        assertFalse(partitionState.update(3, 207), "no sync");
        partitionState.reset();
        assertTrue(partitionState.update(4, 3), "downstream reset");
        partitionState.reset();
        assertFalse(partitionState.update(5, 4), "no sync");
        assertTrue(partitionState.update(7, 6), "sync");
        assertTrue(partitionState.update(7, 6), "sync");
        assertTrue(partitionState.update(8, 7), "sync");
        assertTrue(partitionState.update(10, 57), "sync");
        partitionState.reset();
        assertFalse(partitionState.update(11, 58), "sync");
        assertFalse(partitionState.shouldSyncOffsets, "should sync offsets to false");
    }

    @Test
    public void testZeroOffsetSync() {
        OffsetSyncWriter.PartitionState partitionState = new OffsetSyncWriter.PartitionState(0);

        // if max offset lag is zero, should always emit offset syncs
        assertTrue(partitionState.update(0, 100), "zeroOffsetSync downStreamOffset 100 is incorrect");
        assertTrue(partitionState.shouldSyncOffsets, "should sync offsets");
        partitionState.reset();
        assertFalse(partitionState.shouldSyncOffsets, "should sync offsets to false");
        assertTrue(partitionState.update(2, 102), "zeroOffsetSync downStreamOffset 102 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(3, 153), "zeroOffsetSync downStreamOffset 153 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(4, 154), "zeroOffsetSync downStreamOffset 154 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(5, 155), "zeroOffsetSync downStreamOffset 155 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(6, 207), "zeroOffsetSync downStreamOffset 207 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(2, 208), "zeroOffsetSync downStreamOffset 208 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(3, 209), "zeroOffsetSync downStreamOffset 209 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(4, 3), "zeroOffsetSync downStreamOffset 3 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(5, 4), "zeroOffsetSync downStreamOffset 4 is incorrect");
        assertTrue(partitionState.update(7, 6), "zeroOffsetSync downStreamOffset 6 is incorrect");
        assertTrue(partitionState.update(7, 6), "zeroOffsetSync downStreamOffset 6 is incorrect");
        assertTrue(partitionState.update(8, 7), "zeroOffsetSync downStreamOffset 7 is incorrect");
        assertTrue(partitionState.update(10, 57), "zeroOffsetSync downStreamOffset 57 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(11, 58), "zeroOffsetSync downStreamOffset 58 is incorrect");
    }

    @Test
    public void testPoll() {
        // Create a consumer mock
        byte[] key1 = "abc".getBytes();
        byte[] value1 = "fgh".getBytes();
        byte[] key2 = "123".getBytes();
        byte[] value2 = "456".getBytes();
        List<ConsumerRecord<byte[], byte[]>> consumerRecordsList =  new ArrayList<>();
        String topicName = "test";
        String headerKey = "key";
        RecordHeaders headers = new RecordHeaders(new Header[] {
            new RecordHeader(headerKey, "value".getBytes()),
        });
        consumerRecordsList.add(new ConsumerRecord<>(topicName, 0, 0, System.currentTimeMillis(),
                TimestampType.CREATE_TIME, key1.length, value1.length, key1, value1, headers, Optional.empty()));
        consumerRecordsList.add(new ConsumerRecord<>(topicName, 1, 1, System.currentTimeMillis(),
                TimestampType.CREATE_TIME, key2.length, value2.length, key2, value2, headers, Optional.empty()));
        final TopicPartition tp = new TopicPartition(topicName, 0);
        ConsumerRecords<byte[], byte[]> consumerRecords =
                new ConsumerRecords<>(Map.of(tp, consumerRecordsList), Map.of(tp, new OffsetAndMetadata(2, Optional.empty(), "")));

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        when(consumer.poll(any())).thenReturn(consumerRecords);

        MirrorSourceMetrics metrics = mock(MirrorSourceMetrics.class);

        String sourceClusterName = "cluster1";
        ReplicationPolicy replicationPolicy = new DefaultReplicationPolicy();
        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(consumer, metrics, sourceClusterName,
                replicationPolicy, null);
        List<SourceRecord> sourceRecords = mirrorSourceTask.poll();

        assertEquals(2, sourceRecords.size());
        for (int i = 0; i < sourceRecords.size(); i++) {
            SourceRecord sourceRecord = sourceRecords.get(i);
            ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecordsList.get(i);
            assertEquals(consumerRecord.key(), sourceRecord.key(),
                    "consumerRecord key does not equal sourceRecord key");
            assertEquals(consumerRecord.value(), sourceRecord.value(),
                    "consumerRecord value does not equal sourceRecord value");
            // We expect that the topicname will be based on the replication policy currently used
            assertEquals(replicationPolicy.formatRemoteTopic(sourceClusterName, topicName),
                    sourceRecord.topic(), "topicName not the same as the current replicationPolicy");
            // We expect that MirrorMaker will keep the same partition assignment
            assertEquals(consumerRecord.partition(), sourceRecord.kafkaPartition().intValue(),
                    "partition assignment not the same as the current replicationPolicy");
            // Check header values
            List<Header> expectedHeaders = new ArrayList<>();
            consumerRecord.headers().forEach(expectedHeaders::add);
            List<org.apache.kafka.connect.header.Header> taskHeaders = new ArrayList<>();
            sourceRecord.headers().forEach(taskHeaders::add);
            compareHeaders(expectedHeaders, taskHeaders);
        }
    }

    @Test
    public void testSeekBehaviorDuringStart() {
        // Setting up mock behavior.
        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> mockConsumer = mock(KafkaConsumer.class);

        SourceTaskContext mockSourceTaskContext = mock(SourceTaskContext.class);
        OffsetStorageReader mockOffsetStorageReader = mock(OffsetStorageReader.class);
        when(mockSourceTaskContext.offsetStorageReader()).thenReturn(mockOffsetStorageReader);

        Set<TopicPartition> topicPartitions = new HashSet<>(Arrays.asList(
                new TopicPartition("previouslyReplicatedTopic", 8),
                new TopicPartition("previouslyReplicatedTopic1", 0),
                new TopicPartition("previouslyReplicatedTopic", 1),
                new TopicPartition("newTopicToReplicate1", 1),
                new TopicPartition("newTopicToReplicate1", 4),
                new TopicPartition("newTopicToReplicate2", 0)
        ));

        // Mock beginning and end offsets to prevent false positives in startup topic reset detection.
        // During startup validation, MirrorSourceTask.initializeConsumer() invokes consumer.beginningOffsets() 
        // and consumer.endOffsets(). Without these mock stubs, Mockito returns empty maps by default,
        // which triggers the recreated-topic heuristic (earliestAvailable=0, lastCommitted>0, lastCommitted>endOffset)
        // and incorrectly resets seeking offsets to 0L instead of seeking to committedOffset + 1.
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        for (TopicPartition tp : topicPartitions) {
            beginningOffsets.put(tp, 0L);
            endOffsets.put(tp, 100L);
        }
        when(mockConsumer.beginningOffsets(any())).thenReturn(beginningOffsets);
        when(mockConsumer.endOffsets(any())).thenReturn(endOffsets);

        long arbitraryCommittedOffset = 4L;
        long offsetToSeek = arbitraryCommittedOffset + 1L;
        when(mockOffsetStorageReader.offset(anyMap())).thenAnswer(testInvocation -> {
            Map<String, Object> topicPartitionOffsetMap = testInvocation.getArgument(0);
            String topicName = topicPartitionOffsetMap.get("topic").toString();

            // Only return the offset for previously replicated topics.
            // For others, there is no value set.
            if (topicName.startsWith("previouslyReplicatedTopic")) {
                topicPartitionOffsetMap.put("offset", arbitraryCommittedOffset);
            }
            return topicPartitionOffsetMap;
        });

        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(mockConsumer, null, null,
                new DefaultReplicationPolicy(), null);
        mirrorSourceTask.initialize(mockSourceTaskContext);

        // Call test subject
        mirrorSourceTask.initializeConsumer(topicPartitions);

        // Verifications
        // Ensure all the topic partitions are assigned to consumer
        verify(mockConsumer, times(1)).assign(topicPartitions);

        // Ensure seek is only called for previously committed topic partitions.
        verify(mockConsumer, times(1))
                .seek(new TopicPartition("previouslyReplicatedTopic", 8), offsetToSeek);
        verify(mockConsumer, times(1))
                .seek(new TopicPartition("previouslyReplicatedTopic", 1), offsetToSeek);
        verify(mockConsumer, times(1))
                .seek(new TopicPartition("previouslyReplicatedTopic1", 0), offsetToSeek);

        // verifyNoMoreInteractions(mockConsumer) is omitted here because initializeConsumer()
        // now makes additional startup validation calls (beginningOffsets and endOffsets)
        // to detect log truncation/topic reset scenarios, which are outside of the original test scope.
    }

    @Test
    public void testCommitRecordWithNullMetadata() {
        // Create a consumer mock
        byte[] key1 = "abc".getBytes();
        byte[] value1 = "fgh".getBytes();
        String topicName = "test";
        String headerKey = "key";
        RecordHeaders headers = new RecordHeaders(new Header[] {
            new RecordHeader(headerKey, "value".getBytes()),
        });

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked")
        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        MirrorSourceMetrics metrics = mock(MirrorSourceMetrics.class);

        String sourceClusterName = "cluster1";
        ReplicationPolicy replicationPolicy = new DefaultReplicationPolicy();
        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(consumer, metrics, sourceClusterName,
                replicationPolicy, null);

        SourceRecord sourceRecord = mirrorSourceTask.convertRecord(new ConsumerRecord<>(topicName, 0, 0, System.currentTimeMillis(),
                TimestampType.CREATE_TIME, key1.length, value1.length, key1, value1, headers, Optional.empty()));

        // Expect that commitRecord will not throw an exception
        mirrorSourceTask.commitRecord(sourceRecord, null);
    }

    @Test
    public void testSendSyncEvent() {
        byte[] recordKey = "key".getBytes();
        byte[] recordValue = "value".getBytes();
        long maxOffsetLag = 50;
        int recordPartition = 0;
        int recordOffset = 0;
        int metadataOffset = 100;
        String topicName = "topic";
        String sourceClusterName = "sourceCluster";

        RecordHeaders headers = new RecordHeaders();
        ReplicationPolicy replicationPolicy = new DefaultReplicationPolicy();

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        MirrorSourceMetrics metrics = mock(MirrorSourceMetrics.class);
        PartitionState partitionState = new PartitionState(maxOffsetLag);
        Map<TopicPartition, PartitionState> partitionStates = new HashMap<>();
        OffsetSyncWriter offsetSyncWriter = mock(OffsetSyncWriter.class);
        when(offsetSyncWriter.maxOffsetLag()).thenReturn(maxOffsetLag);
        doNothing().when(offsetSyncWriter).firePendingOffsetSyncs();
        doNothing().when(offsetSyncWriter).promoteDelayedOffsetSyncs();

        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(consumer, metrics, sourceClusterName,
                replicationPolicy, offsetSyncWriter);

        SourceRecord sourceRecord = mirrorSourceTask.convertRecord(new ConsumerRecord<>(topicName, recordPartition,
                recordOffset, System.currentTimeMillis(), TimestampType.CREATE_TIME, recordKey.length,
                recordValue.length, recordKey, recordValue, headers, Optional.empty()));

        TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(sourceRecord.sourcePartition());
        partitionStates.put(sourceTopicPartition, partitionState);
        RecordMetadata recordMetadata = new RecordMetadata(sourceTopicPartition, metadataOffset, 0, 0, 0, recordPartition);
        doNothing().when(offsetSyncWriter).maybeQueueOffsetSyncs(eq(sourceTopicPartition), eq((long) recordOffset), eq(recordMetadata.offset()));

        mirrorSourceTask.commitRecord(sourceRecord, recordMetadata);
        // We should have dispatched this sync to the producer
        verify(offsetSyncWriter, times(1)).maybeQueueOffsetSyncs(eq(sourceTopicPartition), eq((long) recordOffset), eq(recordMetadata.offset()));
        verify(offsetSyncWriter, times(1)).firePendingOffsetSyncs();

        mirrorSourceTask.commit();
        // No more syncs should take place; we've been able to publish all of them so far
        verify(offsetSyncWriter, times(1)).promoteDelayedOffsetSyncs();
        verify(offsetSyncWriter, times(2)).firePendingOffsetSyncs();
    }

    private void compareHeaders(List<Header> expectedHeaders, List<org.apache.kafka.connect.header.Header> taskHeaders) {
        assertEquals(expectedHeaders.size(), taskHeaders.size());
        for (int i = 0; i < expectedHeaders.size(); i++) {
            Header expectedHeader = expectedHeaders.get(i);
            org.apache.kafka.connect.header.Header taskHeader = taskHeaders.get(i);
            assertEquals(expectedHeader.key(), taskHeader.key(),
                    "taskHeader's key expected to equal " + taskHeader.key());
            assertEquals(expectedHeader.value(), taskHeader.value(),
                    "taskHeader's value expected to equal " + taskHeader.value().toString());
        }
    }

    @Test
    public void testDataLossDetectedAtStartup() {
        TopicPartition tp = new TopicPartition("test-topic", 0);
        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> mockConsumer = mock(KafkaConsumer.class);
        // beginning=200, lastCommitted=100 → 200 > 101 → data loss (not a
        // reset)
        when(mockConsumer.beginningOffsets(any()))
            .thenReturn(Collections.singletonMap(tp, 200L));
        // end=300 → lastCommitted+1=101 is NOT > endOffset, so reset branch is
        // skipped
        when(mockConsumer.endOffsets(any()))
            .thenReturn(Collections.singletonMap(tp, 300L));

        SourceTaskContext mockSourceTaskContext = mock(SourceTaskContext.class);
        OffsetStorageReader mockOffsetStorageReader =
            mock(OffsetStorageReader.class);
        when(mockSourceTaskContext.offsetStorageReader())
            .thenReturn(mockOffsetStorageReader);

        when(mockOffsetStorageReader.offset(anyMap())).thenAnswer(inv -> {
            Map<String, Object> m = new HashMap<>(inv.getArgument(0));
            m.put("offset", 100L);
            return m;
        });

        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(mockConsumer,
            null, "primary", new DefaultReplicationPolicy(), null);
        mirrorSourceTask.initialize(mockSourceTaskContext);

        assertThrows(DataLossException.class,
            ()
                -> mirrorSourceTask.initializeConsumer(
                    Collections.singleton(tp)));
    }

    @Test
    public void testStartupTopicResetDetected() {
        // Simulates: MM2 had committed offset 999, topic deleted+recreated (5
        // new msgs). endOffset=5, beginning=0, lastCommitted=999 → reset
        // detected at startup.
        TopicPartition tp = new TopicPartition("test-topic", 0);
        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> mockConsumer = mock(KafkaConsumer.class);
        when(mockConsumer.beginningOffsets(any()))
            .thenReturn(Collections.singletonMap(tp, 0L));
        when(mockConsumer.endOffsets(any()))
            .thenReturn(Collections.singletonMap(tp, 5L));

        SourceTaskContext mockSourceTaskContext = mock(SourceTaskContext.class);
        OffsetStorageReader mockOffsetStorageReader =
            mock(OffsetStorageReader.class);
        when(mockSourceTaskContext.offsetStorageReader())
            .thenReturn(mockOffsetStorageReader);

        when(mockOffsetStorageReader.offset(anyMap())).thenAnswer(inv -> {
            Map<String, Object> m = new HashMap<>(inv.getArgument(0));
            m.put(
                "offset", 999L); // prior committed offset from before the reset
            return m;
        });

        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(mockConsumer,
            null, "primary", new DefaultReplicationPolicy(), null);
        mirrorSourceTask.initialize(mockSourceTaskContext);

        // Should NOT throw — resets gracefully to offset 0
        mirrorSourceTask.initializeConsumer(Collections.singleton(tp));

        // seekToBeginning must have been called with the reset partition
        verify(mockConsumer, times(1))
            .seekToBeginning(Collections.singleton(tp));
        // Final seek must land at 0
        verify(mockConsumer, times(1)).seek(tp, 0L);
    }

    @Test
    public void testCompactedTopicOffsetGapIsNotDataLoss() {
        TopicPartition tp = new TopicPartition("compacted-topic", 0);
        List<ConsumerRecord<byte[], byte[]>> recordsList = new ArrayList<>();
        recordsList.add(new ConsumerRecord<>(
            "compacted-topic", 0, 10L, null, "compacted-msg".getBytes()));
        ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(
            Map.of(tp, recordsList), Collections.emptyMap());

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> mockConsumer = mock(KafkaConsumer.class);
        when(mockConsumer.poll(any())).thenReturn(consumerRecords);

        MirrorSourceTask task =
            new MirrorSourceTask(mockConsumer, mock(MirrorSourceMetrics.class),
                "primary", new DefaultReplicationPolicy(), null);
        task.markTopicAsCompacted("compacted-topic");
        task.putExpectedOffset(tp, 5L); // offset gap: expected 5, got 10 — compacted topic, should be allowed

        List<SourceRecord> result = task.poll();
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testOffsetOutOfRangeThrowsDataLossException() {
        TopicPartition tp = new TopicPartition("test-topic", 0);
        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> mockConsumer = mock(KafkaConsumer.class);
        when(mockConsumer.poll(any()))
            .thenThrow(new OffsetOutOfRangeException(
                Collections.singletonMap(tp, 100L)));
        when(mockConsumer.beginningOffsets(any()))
            .thenReturn(Collections.singletonMap(tp, 300L));

        MirrorSourceTask task = new MirrorSourceTask(mockConsumer, null,
            "primary", new DefaultReplicationPolicy(), null);
        task.putExpectedOffset(tp, 100L);

        assertThrows(DataLossException.class, task::poll);
    }

    @Test
    public void testMidStreamTopicResetContinuesProcessingRecord() {
        // When a topic is deleted & recreated, offsets restart from 0.
        // The record that triggers detection (at offset 0) should NOT be
        // dropped—it is valid post-reset data.
        TopicPartition tp = new TopicPartition("test-topic", 0);
        byte[] key = "k".getBytes();
        byte[] value = "v".getBytes();
        List<ConsumerRecord<byte[], byte[]>> recordsList = new ArrayList<>();
        // Simulate reset: we expected offset 500, but got offset 0 (topic
        // recreated)
        recordsList.add(new ConsumerRecord<>("test-topic", 0, 0L,
            System.currentTimeMillis(), TimestampType.CREATE_TIME, key.length,
            value.length, key, value, new RecordHeaders(), Optional.empty()));
        ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(
            Map.of(tp, recordsList), Collections.emptyMap());

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> mockConsumer = mock(KafkaConsumer.class);
        when(mockConsumer.poll(any())).thenReturn(consumerRecords);

        MirrorSourceTask task =
            new MirrorSourceTask(mockConsumer, mock(MirrorSourceMetrics.class),
                "primary", new DefaultReplicationPolicy(), null);
        task.putExpectedOffset(tp, 500L); // expected offset 500, will receive 0

        // The record at offset 0 should be processed (not silently dropped)
        // Note: verifyOffsetSequence returns void now
        List<SourceRecord> result = task.poll();
        assertNotNull(result);
        assertEquals(1, result.size());
        // seekToBeginning is called so the next poll starts cleanly from 0
        verify(mockConsumer, times(1))
            .seekToBeginning(Collections.singletonList(tp));
    }

    @Test
    public void testOffsetOutOfRangeConfirmedTopicResetSeeksToBeginning() {
        // This tests the startup-time recovery path via
        // OffsetOutOfRangeException. When beginning offset is 0 and expected >
        // 0, topic was recreated.
        TopicPartition tp = new TopicPartition("test-topic", 0);
        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> mockConsumer = mock(KafkaConsumer.class);
        when(mockConsumer.poll(any()))
            .thenThrow(new OffsetOutOfRangeException(
                Collections.singletonMap(tp, 500L)));
        when(mockConsumer.beginningOffsets(any()))
            .thenReturn(Collections.singletonMap(tp, 0L));

        MirrorSourceTask task = new MirrorSourceTask(mockConsumer, null,
            "primary", new DefaultReplicationPolicy(), null);
        task.putExpectedOffset(tp, 500L);

        // handleExceptionBounds seeks to beginning and poll returns null (no
        // records yet)
        List<SourceRecord> result = task.poll();
        assertNull(result);
        verify(mockConsumer, times(1))
            .seekToBeginning(Collections.singletonList(tp));
    }
}
