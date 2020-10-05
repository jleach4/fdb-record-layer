/*
 * RLLuceneDirectory.java
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class FDBDirectory extends Directory {
    private static final Logger LOG = LoggerFactory.getLogger(FDBDirectory.class);
    public static final int DEFAULT_BLOCK_SIZE = 10 * 1024;
    public static final int MAXIMUM_BLOCK_SIZE = 10 * 10 * 1024;
    private final AtomicLong nextTempFileCounter = new AtomicLong();
    public Transaction txn;
    public final Subspace subspace;
    private final Subspace sequenceSubspace;
    private final Subspace metaSubspace;
    private final Subspace dataSubspace;
    private final byte[] sequenceSubspaceKey;

    private final LockFactory lockFactory;
    private final int blockSize;
    private static final Set<String> EMPTY_SET = new HashSet<>();
    private AtomicLong atomicLong;
    private boolean hasWritten;
    private CompletableFuture<Void> setIncrement;

    public FDBDirectory(Subspace subspace, Transaction txn) {
        this(subspace, txn, NoLockFactory.INSTANCE);
    }

    FDBDirectory(Subspace subspace, Transaction txn, LockFactory lockFactory) {
        this(subspace, txn, lockFactory, DEFAULT_BLOCK_SIZE);
    }

    FDBDirectory(Subspace subspace, Transaction txn, LockFactory lockFactory, int blockSize) {
        assert subspace != null;
        assert txn != null;
        assert lockFactory != null;
        this.txn = txn;
        this.subspace = subspace;
        this.sequenceSubspace = subspace.subspace(Tuple.from("seq-"));
        this.sequenceSubspaceKey = sequenceSubspace.pack();
        this.metaSubspace = subspace.subspace(Tuple.from("meta-"));
        this.dataSubspace = subspace.subspace(Tuple.from("data-"));
        this.lockFactory = lockFactory;
        this.blockSize = blockSize;
    }

    public CompletableFuture<Void> setIncrement() {
        return txn.get(sequenceSubspaceKey).thenAcceptAsync(
                (value) -> {
                    if (value == null) {
                        atomicLong = new AtomicLong(1L);
                    } else {
                        long sequence = Tuple.fromBytes(value).getLong(0);
                        atomicLong = new AtomicLong(sequence+1);
                    }
                });
    }

    public long getIncrement() {
        if (setIncrement==null) {
            setIncrement = setIncrement();
        }
        setIncrement.join();
        return atomicLong.incrementAndGet();
    }

    public CompletableFuture<FDBLuceneFileReference> getFDBLuceneFileReference(final String name) {
        LOG.trace("getFDBLuceneFileReference {}", name);
        return txn.get(metaSubspace.pack(name))
                    .thenApplyAsync( (value) ->
                        value == null ? null : new FDBLuceneFileReference(Tuple.fromBytes(value))
                    );
    }

    public void writeFDBLuceneFileReference(final String name, final FDBLuceneFileReference reference) {
        LOG.trace("writeFDBLuceneFileReference {}", reference);
        hasWritten = true;
        txn.set(metaSubspace.pack(name), reference.getTuple().pack());
    }

    public void writeData(long id, int block, byte[] value) {
        LOG.trace("writeData id={}, block={}, valueSize={}", id, block, value.length);
        hasWritten = true;
        txn.set(dataSubspace.pack(Tuple.from(id, block)), value);
    }

    public CompletableFuture<byte[]> seekData(CompletableFuture<FDBLuceneFileReference> referenceFuture, int block) {
        LOG.trace("seekData block={}", block);
        return referenceFuture
                .thenApplyAsync( (reference) ->
                        txn.get(dataSubspace.pack(Tuple.from(reference.getId(), block))).join()
                );
    }

    @Override
    public String[] listAll() {
        List<String> outList = new ArrayList<String>();
        for(KeyValue kv : txn.getRange(metaSubspace.range())) {
            outList.add(metaSubspace.unpack(kv.getKey()).getString(0));
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("listAllFiles -> {}", String.join(", ", outList));
        }
        return outList.toArray(new String[outList.size()]);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        LOG.trace("deleteFile -> {}", name);
        hasWritten = true;
        getFDBLuceneFileReference(name).thenAcceptAsync(
                (value) -> {
                    if(value == null) {
                        throw new RuntimeException(new NoSuchFileException(name));
                    }
                    txn.clear(metaSubspace.pack(name));
                    txn.clear(dataSubspace.subspace(Tuple.from(value.getId())).range());
                }
        ).join();
    }

    @Override
    public long fileLength(String name) throws NoSuchFileException {
        LOG.trace("fileLength -> {}", name);
        FDBLuceneFileReference reference = getFDBLuceneFileReference(name).join();
        if (reference == null) {
            throw new NoSuchFileException(name);
        }
        return reference.getSize();
    }

    @Override
    public IndexOutput createOutput(final String name, final IOContext ioContext) throws IOException {
        LOG.trace("createOutput -> {}", name);
        hasWritten = true;
        return new FDBIndexOutput(name, name, this);
    }

    @Override
    public IndexOutput createTempOutput(final String prefix, final String suffix, final IOContext ioContext) throws IOException {
        LOG.trace("createTempOutput -> prefix={}, suffix={}", prefix, suffix);
        hasWritten = true;
        return createOutput(getTempFileName(prefix, suffix, this.nextTempFileCounter.getAndIncrement()), ioContext);
    }

    protected static String getTempFileName(String prefix, String suffix, long counter) {
        return IndexFileNames.segmentFileName(prefix, suffix + "_" + Long.toString(counter, 36), "tmp");
    }

    @Override
    public void sync(final Collection<String> collection) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("sync -> {}", String.join(", ", collection));
        }
    }

    @Override
    public void syncMetaData() throws IOException {
        LOG.trace("syncMetaData hasWritten={}",hasWritten);
        if (hasWritten) {
            txn.set(sequenceSubspaceKey, Tuple.from(atomicLong.getAndIncrement()).pack());
        }
    }

    @Override
    public void rename(final String source, final String dest) throws IOException {
        LOG.trace("rename -> source={}, dest={}", source, dest);
        final byte[] key = metaSubspace.pack(source);
        txn.get(key).thenAcceptAsync( (value) -> {
                    txn.set(metaSubspace.pack(dest), value);
                    txn.clear(key);
                }).join();
    }

    @Override
    public IndexInput openInput(final String name, final IOContext ioContext) throws IOException {
        LOG.trace("openInput -> name={}", name);
        return new FDBIndexInput(name, this);
    }

    @Override
    public Lock obtainLock(final String s) throws IOException {
        LOG.trace("obtainLock -> {}", s);
        return lockFactory.obtainLock(null,s);
    }

    @Override
    public void close() throws IOException {
        LOG.trace("close");
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        LOG.trace("getPendingDeletions");
        return EMPTY_SET;
    }

    public int getBlockSize() {
        return blockSize;
    }
}
