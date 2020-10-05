/*
 * LuceneIndexMaintainer.java
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.IteratorCursor;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;
import com.apple.foundationdb.record.provider.foundationdb.indexes.InvalidIndexEntry;
import com.apple.foundationdb.record.provider.foundationdb.indexes.StandardIndexMaintainer;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class LuceneIndexMaintainer extends StandardIndexMaintainer {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexMaintainer.class);
    private final Directory directory;
    private final Analyzer analyzer;
    private final String TEXT_FIELD_NAME = "t";
    private final String PRIMARY_KEY_FIELD_NAME = "p";
    private final String PRIMARY_KEY_SEARCH_NAME = "s";
    private final int textPosition;

    public LuceneIndexMaintainer(final IndexMaintainerState state) {
        super(state);
        this.directory = new FDBDirectory(state.indexSubspace, state.transaction);
        this.analyzer = new StandardAnalyzer();
        this.textPosition = textFieldPosition(state.index.getRootExpression());
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType, @Nonnull TupleRange range,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
        LOG.debug("scan scanType={}", scanType);
        Term term = new Term(TEXT_FIELD_NAME, range.getLow().getString(0));
        Query query = new TermQuery(term);
        IndexReader indexReaderRef = null;
        try {
            final IndexReader indexReader = DirectoryReader.open(directory);
            indexReaderRef = indexReader;
            IndexSearcher searcher = new IndexSearcher(indexReader);
            TopDocs topDocs = searcher.search(query, 10);
            return new IteratorCursor<>(getExecutor(),
                    Arrays.stream(topDocs.scoreDocs).map(scoreDoc -> {
                            try {
                                Document document = searcher.doc(scoreDoc.doc);
                                IndexableField primaryKey = document.getField(PRIMARY_KEY_FIELD_NAME);
                                IndexableField textField = document.getField(TEXT_FIELD_NAME);
                                BytesRef pk = primaryKey.binaryValue();
                                LOG.trace("document={}", document);
                                return new IndexEntry(state.index, Tuple.fromBytes(pk.bytes, pk.offset, pk.length), Tuple.from(textField.stringValue()));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            }).iterator(), () -> indexReader.close());
        } catch (IOException ioe) {
            IOUtils.closeWhileHandlingException(indexReaderRef);
            throw new RuntimeException(ioe);
        }
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Void> update(@Nullable FDBIndexableRecord<M> oldRecord,
                                                              @Nullable FDBIndexableRecord<M> newRecord) {
        LOG.debug("update oldRecord={}, newRecord{}", oldRecord, newRecord);
        return super.update(oldRecord, newRecord);
    }

    @Nonnull
    @Override
    protected <M extends Message> CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                          final boolean remove,
                                                                          @Nonnull final List<IndexEntry> indexEntries) {
        LOG.debug("updateIndexKeys savedRecord={}, remove=={}, indexEntries={}", savedRecord, remove, indexEntries);
        if (indexEntries.isEmpty()) {
            return AsyncUtil.DONE;
        }
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        try (IndexWriter writer = new IndexWriter(directory, indexWriterConfig)) {
            indexEntries.stream().forEach( (entry ) -> {
                try {
                    if (remove) {
                        Query query = SortedDocValuesField.newSlowExactQuery(PRIMARY_KEY_SEARCH_NAME, new BytesRef(savedRecord.getPrimaryKey().pack()));
                        writer.deleteDocuments(query);
                    } else {
                        Document document = new Document();
                        document.add(new StoredField(PRIMARY_KEY_FIELD_NAME, savedRecord.getPrimaryKey().pack()));
                        document.add(new SortedDocValuesField(PRIMARY_KEY_SEARCH_NAME, new BytesRef(savedRecord.getPrimaryKey().pack())));
                        document.add(new TextField(TEXT_FIELD_NAME, entry.getKey().getString(textPosition), Field.Store.YES));
                            writer.addDocument(document);
                    }
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return AsyncUtil.DONE;
    }

    /*
    TODO Syntax Highlighting Scan options JL
    https://lucene.apache.org/solr/guide/6_6/highlighting.html

        //Uses HTML &lt;B&gt;&lt;/B&gt; tag to highlight the searched terms
        Formatter formatter = new SimpleHTMLFormatter();

        //It scores text fragments by the number of unique query terms found
        //Basically the matching score in layman terms
        QueryScorer scorer = new QueryScorer(query);

        //used to markup highlighted terms found in the best sections of a text
        Highlighter highlighter = new Highlighter(formatter, scorer);
     */

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scanUniquenessViolations(@Nonnull TupleRange range, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        LOG.debug("scanUniquenessViolations");
        return RecordCursor.empty();
    }

    @Nonnull
    @Override
    public RecordCursor<InvalidIndexEntry> validateEntries(@Nullable byte[] continuation, @Nullable ScanProperties scanProperties) {
        LOG.debug("validateEntries");
        return RecordCursor.empty();
    }

    @Override
    public boolean canEvaluateRecordFunction(@Nonnull IndexRecordFunction<?> function) {
        LOG.debug("canEvaluateRecordFunction() function={}", function);
        return false;
    }

    @Nonnull
    @Override
    public <T, M extends Message> CompletableFuture<T> evaluateRecordFunction(@Nonnull EvaluationContext context,
                                                                              @Nonnull IndexRecordFunction<T> function,
                                                                              @Nonnull FDBRecord<M> record) {
        LOG.warn("evaluateRecordFunction() function={}", function);
        return unsupportedRecordFunction(function);
    }

    @Override
    public boolean canEvaluateAggregateFunction(@Nonnull IndexAggregateFunction function) {
        LOG.trace("canEvaluateAggregateFunction() function={}", function);
        return false;
    }

    @Nonnull
    @Override
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull IndexAggregateFunction function,
                                                              @Nonnull TupleRange range,
                                                              @Nonnull IsolationLevel isolationLevel) {
        LOG.warn("evaluateAggregateFunction() function={}", function);
        return unsupportedAggregateFunction(function);
    }

    @Override
    public boolean isIdempotent() {
        LOG.trace("isIdempotent()");
        return false;
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> addedRangeWithKey(@Nonnull Tuple primaryKey) {
        LOG.debug("addedRangeWithKey primaryKey={}", primaryKey);
        return AsyncUtil.READY_FALSE;
    }

    @Override
    public boolean canDeleteWhere(@Nonnull QueryToKeyMatcher matcher, @Nonnull Key.Evaluated evaluated) {
        LOG.debug("canDeleteWhere matcher={}", matcher);
        return true;
    }

    @Override
    public CompletableFuture<Void> deleteWhere(Transaction tr, @Nonnull Tuple prefix) {
        LOG.debug("deleteWhere transaction={}, prefix", tr, prefix);
        return AsyncUtil.DONE;
    }

    @Override
    public CompletableFuture<IndexOperationResult> performOperation(@Nonnull IndexOperation operation) {
        LOG.debug("performOperation operation={}", operation);
        return CompletableFuture.completedFuture(new IndexOperationResult() {
        });
    }

    // Gets the position of the text field this index is tokenizing from within the
    // index's expression. This is the first column of the index expression after
    // all grouping columns (or the first column if there are no grouping columns).
    static int textFieldPosition(@Nonnull KeyExpression expression) {
        if (expression instanceof GroupingKeyExpression) {
            return ((GroupingKeyExpression) expression).getGroupingCount();
        } else {
            return 0;
        }
    }

}
