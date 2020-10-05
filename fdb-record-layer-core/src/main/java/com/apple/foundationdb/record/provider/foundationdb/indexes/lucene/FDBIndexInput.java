/*
 * FDBIndexInput.java
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

import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Class that handles reading data cut into blocks (KeyValue) backed by an FDB keyspace.
 *
 */
public class FDBIndexInput extends IndexInput {
    private static final Logger LOG = LoggerFactory.getLogger(FDBIndexInput.class);
    private String resourceDescription;
    private final FDBDirectory fdbDirectory;
    private final CompletableFuture<FDBLuceneFileReference> reference;
    private long position;
    private CompletableFuture<byte[]> currentData;
    private int currentBlock;
    private long initialOffset;

    /**
     * Constructor to create an FDBIndexInput from a file referenced in the metadata keyspace.
     *
     * This constructor will begin an asynchronous query (lookahead) to the first block.
     *
     * @param resourceDescription
     * @param fdbDirectory
     * @throws IOException
     */
    public FDBIndexInput(final String resourceDescription, final FDBDirectory fdbDirectory) throws IOException {
        this(resourceDescription, fdbDirectory, fdbDirectory.getFDBLuceneFileReference(resourceDescription), 0L,
                0L, 0, null);
    }

    /**
     * Constructor that is utilized by splice calls to take into account initial offsets and modifications to length.
     *
     * @param resourceDescription
     * @param fdbDirectory
     * @param reference
     * @param initalOffset
     * @param position
     * @param currentBlock
     * @param currentData
     * @throws IOException
     */
    public FDBIndexInput(final String resourceDescription, final FDBDirectory fdbDirectory,
                         CompletableFuture<FDBLuceneFileReference> reference, long initalOffset, long position,
                         int currentBlock, CompletableFuture<byte[]> currentData) throws IOException {
        super(resourceDescription);
        LOG.trace("init() -> {}", resourceDescription);
        this.resourceDescription = resourceDescription;
        this.fdbDirectory = fdbDirectory;
        this.reference = reference;
        this.position = position;
        this.currentBlock = currentBlock;
        this.currentData = currentData;
        this.initialOffset = initalOffset;
        if (currentData == null) {
            this.currentData = fdbDirectory.seekData(reference, currentBlock);
        } else {
            seek(position);
        }
    }

    /**
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        LOG.trace("close() -> resource={}", resourceDescription);
    }

    /**
     * The current relative position not included any offssets provided by slices.
     *
     * @return
     */
    @Override
    public long getFilePointer() {
        LOG.trace("getFilePointer() -> resource={}, position={}", resourceDescription, position);
        return position;
    }

    /**
     * Seeks to the offset provided.  That actual seek offset will take into account the
     * absolute position taking into account if the IndexInput has been Sliced.
     *
     * If the currentBlock is the same block after the offset, no physical seek will occur.
     *
     * @param offset
     * @throws IOException
     */
    @Override
    public void seek(final long offset) throws IOException {
        LOG.trace("seek -> resource={}, offset={}", resourceDescription, offset);
        if (currentBlock != getBlock(offset)) {
            this.position = offset;
            this.currentBlock = getBlock(position);
            this.currentData = fdbDirectory.seekData(reference, currentBlock); // Physical Seek
        } else {
            this.position = offset;     // Logical Seek
        }
    }

    /**
     * The total length of the input provided by MetaData stored in the metadata keyspace.
     *
     * @return
     */
    @Override
    public long length() {
        LOG.trace("length -> resource={}", resourceDescription);
        return reference.join().getSize();
    }

    /**
     *
     * This takes an existing FDBIndexInput and provides a new initial offset plus
     * a FDBLuceneFileReference that is not <b>not</b> backed in the metadata keyspace.
     *
     * @param sliceDescription
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        LOG.trace("slice -> resource={}, desc={}, offset={}, length={}", resourceDescription, sliceDescription, offset, length);
        return new FDBIndexInput(resourceDescription, fdbDirectory, CompletableFuture.supplyAsync( () ->
                new FDBLuceneFileReference(reference.join().getId(), length, reference.join().getBlockSize())),
                offset + initialOffset, 0L, currentBlock, currentData
                );
    }

    /**
     * The relative position plus any initial offsets provided (slice)
     *
     * @return
     */
    private long absolutePosition() {
        return position + initialOffset;
    }

    /**
     * Read a byte based on the current relative position taking into account the
     * absolute position (slice).
     *
     * If the next byte will be on another block, asynchronously call read ahead.
     *
     * @return
     * @throws IOException
     */
    @Override
    public byte readByte() throws IOException {
        LOG.trace("readByte resource={}", resourceDescription);
        try {
            int probe = (int)(absolutePosition() % reference.join().getBlockSize());
            position++;
            assert currentData.join() != null: "current Data is null: " + resourceDescription + " " + reference.join().getId();
            return currentData.join()[probe];
        } finally {
            if (absolutePosition()  % reference.join().getBlockSize() == 0) {
                currentBlock++;
                this.currentData = fdbDirectory.seekData(reference, currentBlock);
            }
        }
    }

    /**
     * Read bytes based on the offset and taking into account the absolute position.
     *
     * Perform asynchronous read aheads when required.
     *
     * TODO JL (Could perform parallel read aheads if we have a memory management concept.)
     *
     * @param bytes
     * @param offset
     * @param length
     * @throws IOException
     */
    @Override
    public void readBytes(final byte[] bytes, final int offset, final int length) throws IOException {
        LOG.trace("readBytes resource={}, offset={}, length={}", resourceDescription, offset, length);
        int bytesRead = 0;
        long blockSize = reference.join().getBlockSize();
        while(bytesRead < length) {
            long inBlockPosition = (absolutePosition() % blockSize);
            int toRead = (int) (length - bytesRead + inBlockPosition > blockSize? blockSize - inBlockPosition : length - bytesRead);
            System.arraycopy(currentData.join(), (int)inBlockPosition, bytes, bytesRead + offset, toRead);
            bytesRead += toRead;
            position += toRead;
            if (absolutePosition() % blockSize == 0) {
                currentBlock++;
                this.currentData = fdbDirectory.seekData(reference, currentBlock);
            }
        }

    }

    /**
     * Retrieve the appropriate indexed block taking into account the absolute position
     * (possible splice offsets) and dividing by the block size stored in the metadata keyspace.
     *
     * @param position
     * @return
     */
    private int getBlock(long position) {
        return (int) ( (position + initialOffset) / reference.join().getBlockSize());
    }
}
