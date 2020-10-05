/*
 * LuceneIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.provider.foundationdb.indexes.lucene;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@code TEXT} type indexes.
 */
@Tag(Tags.RequiresFDB)
public class LuceneIndexTest extends FDBRecordStoreTestBase {

    private static final Index SIMPLE_TEXT_SUFFIXES = new Index("Simple$text_suffixes", field("text"), IndexTypes.FULL_TEXT,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

    protected void openRecordStore(FDBRecordContext context) throws Exception {
        openRecordStore(context, store -> { });
    }

    protected void openRecordStore(FDBRecordContext context, FDBRecordStoreTestBase.RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .uncheckedOpen();
        setupPlanner(null);
    }

    @Nonnull
    private static FDBStoreTimer getTimer(@Nonnull FDBRecordStore recordStore) {
        final FDBStoreTimer timer = recordStore.getTimer();
        assertNotNull(timer, "store has not been initialized with a timer");
        return timer;
    }

    @Nonnull
    protected FDBRecordStore.Builder getStoreBuilder(@Nonnull FDBRecordContext context, @Nonnull RecordMetaData metaData) {
        return FDBRecordStore.newBuilder()
                .setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION) // set to max to test newest features (unsafe for real deployments)
                .setKeySpacePath(path)
                .setContext(context)
                .setMetaDataProvider(metaData);
    }

    @Test
    public void simpleInsertAndSearch() throws Exception {
        final TestRecordsTextProto.SimpleDocument dylan = TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(1623L)
                .setText("You're an idiot, babe\n" +
                         "It's a wonder that you still know how to breathe")
                .setGroup(2)
                .build();
        final TestRecordsTextProto.SimpleDocument waylon = TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(1547L)
                .setText("There's always one more way to do things and that's your way, and you have a right to try it at least once.")
                .build();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            recordStore.saveRecord(dylan);
            recordStore.saveRecord(waylon);
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, null, TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN);
            assertEquals(1, indexEntries.getCount().join());
        }
    }

    @Test
    public void simpleInsertDeleteAndSearch() throws Exception {
        final TestRecordsTextProto.SimpleDocument dylan = TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(1623L)
                .setText("You're an idiot, babe\n" +
                         "It's a wonder that you still know how to breathe")
                .setGroup(2)
                .build();
        final TestRecordsTextProto.SimpleDocument dylan2 = TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(1624L)
                .setText("You're an idiot, babe\n" +
                         "It's a wonder that you still know how to breathe")
                .setGroup(2)
                .build();
        final TestRecordsTextProto.SimpleDocument waylon = TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(1547L)
                .setText("There's always one more way to do things and that's your way, and you have a right to try it at least once.")
                .build();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            recordStore.saveRecord(dylan);
            recordStore.saveRecord(dylan2);
            recordStore.saveRecord(waylon);
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, null, TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN);
            assertEquals(2, indexEntries.getCount().join());
            assertTrue(recordStore.deleteRecord(Tuple.from(1624L)));
            indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, null, TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN);
            assertEquals(1, indexEntries.getCount().join());
        }
    }

    @Test
    public void testOverflow() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            Directory directory = new FDBDirectory(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), recordStore.ensureContextActive());
            IndexOutput indexOutput = directory.createOutput("OVERFLOW", null);
            Random rd = new Random();
            byte[] in = new byte[10 * 10 * 1024];
            byte[] out = new byte[10 * 10 * 1024];
            rd.nextBytes(in);
            indexOutput.writeBytes(in, in.length);
            indexOutput.close();
            IndexInput indexInput = directory.openInput("OVERFLOW", null);
            indexInput.readBytes(out, 0, out.length);
            assertTrue(Arrays.equals(in, out));
        }
    }
    @Test
    public void testSpanBlocks() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            Directory directory = new FDBDirectory(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), recordStore.ensureContextActive());
            IndexOutput indexOutput = directory.createOutput("OVERFLOW", null);
            Random rd = new Random();
            byte[] in = new byte[10 * 10 * 1024];
            rd.nextBytes(in);
            indexOutput.writeBytes(in, in.length);
            indexOutput.close();
            IndexInput indexInput = directory.openInput("OVERFLOW", null);
            byte[] span = new byte[10];
            indexInput.seek(FDBDirectory.DEFAULT_BLOCK_SIZE - 2);
            indexInput.readBytes(span, 0, 10);
            assertTrue(ByteBuffer.wrap(span).equals(ByteBuffer.wrap(in, FDBDirectory.DEFAULT_BLOCK_SIZE - 2, 10)));
        }
    }

    @Test
    public void testAtBlockBoundary() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            Directory directory = new FDBDirectory(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), recordStore.ensureContextActive());
            IndexOutput indexOutput = directory.createOutput("OVERFLOW", null);
            Random rd = new Random();
            byte[] in = new byte[10 * 10 * 1024];
            rd.nextBytes(in);
            indexOutput.writeBytes(in, in.length);
            indexOutput.close();
            IndexInput indexInput = directory.openInput("OVERFLOW", null);
            byte[] span = new byte[10];
            indexInput.seek(FDBDirectory.DEFAULT_BLOCK_SIZE);
            indexInput.readBytes(span, 0, 10);
            assertTrue(ByteBuffer.wrap(span).equals(ByteBuffer.wrap(in, FDBDirectory.DEFAULT_BLOCK_SIZE, 10)));
        }
    }


    @Test
    public void testConcurrency() throws Exception {
            for (int i = 0; i < 100000; i++) {
                try (FDBRecordContext context = openContext()) {
                    openRecordStore(context, metaDataBuilder -> {
                        metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                        metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
                    });
                    final TestRecordsTextProto.SimpleDocument dylan = TestRecordsTextProto.SimpleDocument.newBuilder()
                        .setDocId(i)
                        .setText("You're an" + i + "  idiot, babe\n" +
                                 "It's a wondesdafdasr wonder z" + i +" sdfdsf" + i + " xxxxxxxxx xxxx xxxxxxxxxxxxxxxxxxx xx" + i + "that you still" + i + " know how to breathe" + i)
                        .setGroup(2)
                        .build();
                recordStore.saveRecord(dylan);
                commit(context);
            }
        }
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, null, TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN);
            assertEquals(10, indexEntries.getCount().join());
        }
    }
}
