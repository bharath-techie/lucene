/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.util.bkd;

import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.DocBaseBitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.Arrays;

final class DocIdsWriterPackedIntsSorted {

    private static final byte CONTINUOUS_IDS = (byte) -2;
    private static final byte BITSET_IDS = (byte) -1;
    private static final byte DELTA_BPV_16 = (byte) 16;
    private static final byte BPV_24 = (byte) 24;
    private static final byte BPV_32 = (byte) 32;

    private static final byte SORTED_WITH_ORDER = (byte) 7; // New format identifier


    private static final byte SORTED_RLE = (byte) 9;
    private static final byte SORTED_DELTA = (byte) 10;


    // These signs are legacy, should no longer be used in the writing side.
    private static final byte LEGACY_DELTA_VINT = (byte) 0;

    private final int[] scratch;
    private final LongsRef scratchLongs = new LongsRef();

    /**
     * IntsRef to be used to iterate over the scratch buffer. A single instance is reused to avoid
     * re-allocating the object. The ints and length fields need to be reset each use.
     *
     * <p>The main reason for existing is to be able to call the {@link
     * IntersectVisitor#visit(IntsRef)} method rather than the {@link IntersectVisitor#visit(int)}
     * method. This seems to make a difference in performance, probably due to fewer virtual calls
     * then happening (once per read call rather than once per doc).
     */
    private final IntsRef scratchIntsRef = new IntsRef();

    {
        // This is here to not rely on the default constructor of IntsRef to set offset to 0
        scratchIntsRef.offset = 0;
    }

    DocIdsWriterPackedIntsSorted(int maxPointsInLeaf) {
        scratch = new int[maxPointsInLeaf];
    }

    void writeDocIds(int[] docIds, int start, int count, DataOutput out) throws IOException {
        boolean useSortedWithOrder = shouldUseSortedWithOrder(docIds, start, count);

        if (useSortedWithOrder) {
            //System.out.println("write sorted");
            writeSortedWithOrder(docIds, start, count, out);
            return;
        }
        //System.out.println("not write sorted");

        // docs can be sorted either when all docs in a block have the same value
        // or when a segment is sorted
        boolean strictlySorted = true;
        int min = docIds[0];
        int max = docIds[0];
        for (int i = 1; i < count; ++i) {
            int last = docIds[start + i - 1];
            int current = docIds[start + i];
            if (last >= current) {
                strictlySorted = false;
            }
            min = Math.min(min, current);
            max = Math.max(max, current);
        }

        int min2max = max - min + 1;
        if (strictlySorted) {
            if (min2max == count) {
                // continuous ids, typically happens when segment is sorted
                out.writeByte(CONTINUOUS_IDS);
                out.writeVInt(docIds[start]);
                return;
            } else if (min2max <= (count << 4)) {
                assert min2max > count : "min2max: " + min2max + ", count: " + count;
                // Only trigger bitset optimization when max - min + 1 <= 16 * count in order to avoid
                // expanding too much storage.
                // A field with lower cardinality will have higher probability to trigger this optimization.
                out.writeByte(BITSET_IDS);
                writeIdsAsBitSet(docIds, start, count, out);
                return;
            }
        }

        if (min2max <= 0xFFFF) {
            out.writeByte(DELTA_BPV_16);
            for (int i = 0; i < count; i++) {
                scratch[i] = docIds[start + i] - min;
            }
            out.writeVInt(min);
            final int halfLen = count >>> 1;
            for (int i = 0; i < halfLen; ++i) {
                scratch[i] = scratch[halfLen + i] | (scratch[i] << 16);
            }
            for (int i = 0; i < halfLen; i++) {
                out.writeInt(scratch[i]);
            }
            if ((count & 1) == 1) {
                out.writeShort((short) scratch[count - 1]);
            }
        } else {
            if (max <= 0xFFFFFF) {
                out.writeByte(BPV_24);
                // write them the same way we are reading them.
                int i;
                for (i = 0; i < count - 7; i += 8) {
                    int doc1 = docIds[start + i];
                    int doc2 = docIds[start + i + 1];
                    int doc3 = docIds[start + i + 2];
                    int doc4 = docIds[start + i + 3];
                    int doc5 = docIds[start + i + 4];
                    int doc6 = docIds[start + i + 5];
                    int doc7 = docIds[start + i + 6];
                    int doc8 = docIds[start + i + 7];
                    long l1 = (doc1 & 0xffffffL) << 40 | (doc2 & 0xffffffL) << 16 | ((doc3 >>> 8) & 0xffffL);
                    long l2 =
                            (doc3 & 0xffL) << 56
                                    | (doc4 & 0xffffffL) << 32
                                    | (doc5 & 0xffffffL) << 8
                                    | ((doc6 >> 16) & 0xffL);
                    long l3 = (doc6 & 0xffffL) << 48 | (doc7 & 0xffffffL) << 24 | (doc8 & 0xffffffL);
                    out.writeLong(l1);
                    out.writeLong(l2);
                    out.writeLong(l3);
                }
                for (; i < count; ++i) {
                    out.writeShort((short) (docIds[start + i] >>> 8));
                    out.writeByte((byte) docIds[start + i]);
                }
            } else {
                out.writeByte(BPV_32);
                for (int i = 0; i < count; i++) {
                    out.writeInt(docIds[start + i]);
                }
            }
        }
    }

    class IndexedValue implements Comparable<IndexedValue> {
        int value;
        int originalIndex;

        IndexedValue(int value, int originalIndex) {
            this.value = value;
            this.originalIndex = originalIndex;
        }

        @Override
        public int compareTo(IndexedValue other) {
            return Integer.compare(this.value, other.value);
        }
    }

    private void writeSortedWithOrder(int[] docIds, int start, int count, DataOutput out) throws IOException {
        out.writeByte(SORTED_WITH_ORDER);

        // Sort docIds while keeping track of original positions
        IndexedValue[] indexedValues = new IndexedValue[count];
        for (int i = 0; i < count; i++) {
            indexedValues[i] = new IndexedValue(docIds[start + i], i);
        }
        Arrays.sort(indexedValues);

        // Write sorted docIds
        int[] sortedDocIds = new int[count];
        for (int i = 0; i < count; i++) {
            sortedDocIds[i] = indexedValues[i].value;
        }

        // Write the sorted values
        if (sortedDocIds[count - 1] - sortedDocIds[0] + 1 == count) {
            out.writeByte(CONTINUOUS_IDS);
            out.writeVInt(sortedDocIds[0]);
        } else {
            writeSortedDeltaDocIds(count, out, sortedDocIds);
        }

        writePositions(count, out, indexedValues);
    }

    private void writeSortedDeltaDocIds(int count, DataOutput out, int[] sortedDocIds) throws IOException {
        out.writeByte(SORTED_DELTA);
        out.writeVInt(count);
        out.writeVInt(sortedDocIds[0]);

        // Calculate deltas
        int[] deltas = new int[count - 1];
        int maxDelta = 0;
        for (int i = 1; i < count; i++) {
            deltas[i - 1] = sortedDocIds[i] - sortedDocIds[i - 1];
            maxDelta = Math.max(maxDelta, deltas[i - 1]);
        }

        // Determine bits per value
        int bitsPerValue = PackedInts.bitsRequired(maxDelta);
        out.writeByte((byte) bitsPerValue);

        // Write deltas
        PackedInts.Writer writer = PackedInts.getWriterNoHeader(out, PackedInts.Format.PACKED, count - 1, bitsPerValue, PackedInts.DEFAULT_BUFFER_SIZE);
        for (int delta : deltas) {
            writer.add(delta);
        }
        writer.finish();
    }

    private void writePositions(int count, DataOutput out, IndexedValue[] indexedValues) throws IOException {
        // Create inverse mapping
        int[] inverseMapping = new int[count];
        for (int i = 0; i < count; i++) {
            inverseMapping[indexedValues[i].originalIndex] = i;
            //System.out.println(indexedValues[i].originalIndex + "===" + indexedValues[i].value);
        }

        // Determine bits per value
        int bitsPerValue = PackedInts.bitsRequired(count - 1);
        out.writeByte((byte) bitsPerValue);

        // Write positions
        PackedInts.Writer writer = PackedInts.getWriterNoHeader(out, PackedInts.Format.PACKED, count, bitsPerValue, PackedInts.DEFAULT_BUFFER_SIZE);
        for (int position : inverseMapping) {
            writer.add(position);
        }
        writer.finish();
    }

    private boolean shouldUseSortedWithOrder(int[] docIds, int start, int count) {
        // Heuristic to decide when to use sorted with order format
        // For example, when the range of values is large but the count is relatively small
        int min = docIds[start];
        int max = min;
        for (int i = 1; i < count; i++) {
            int val = docIds[start + i];
            min = Math.min(min, val);
            max = Math.max(max, val);
        }

        // Use this format when the range is large but count is small
        return (max - min) > count * 4;
    }

    private void readSortedWithOrder(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
        byte valueFormat = in.readByte();
        int[] sortedValues = new int[count];

        switch (valueFormat) {
            case CONTINUOUS_IDS:
                int start = in.readVInt();
                for (int i = 0; i < count; i++) {
                    sortedValues[i] = start + i;
                }
                break;

            case SORTED_DELTA:
                int deltaCount = in.readVInt();
                if (deltaCount != count) {
                    throw new IOException("Count mismatch");
                }

                sortedValues[0] = in.readVInt();
                byte bitsPerValue = in.readByte();

                PackedInts.ReaderIterator deltaIt = PackedInts.getReaderIteratorNoHeader(in, PackedInts.Format.PACKED, PackedInts.VERSION_CURRENT, count - 1, bitsPerValue, PackedInts.DEFAULT_BUFFER_SIZE);
                for (int i = 1; i < count; i++) {
                    sortedValues[i] = sortedValues[i - 1] + (int) deltaIt.next();
                }
                break;

            default:
                throw new IOException("Unknown value format: " + valueFormat);
        }

        // Read positions
        byte posBitsPerValue = in.readByte();
        PackedInts.ReaderIterator posIt = PackedInts.getReaderIteratorNoHeader(in, PackedInts.Format.PACKED, PackedInts.VERSION_CURRENT, count, posBitsPerValue, PackedInts.DEFAULT_BUFFER_SIZE);

        int[] docIds = new int[count];
        for (int i = 0; i < count; i++) {
            int position = (int) posIt.next();
            docIds[i] = sortedValues[position];
        }

        // Verify order and visit docIds
        for (int i = 0; i < count; i++) {
//            if (i > 0 && docIds[i] <= docIds[i - 1]) {
//                throw new AssertionError("docs out of order: last doc=" + docIds[i-1] + " current doc=" + docIds[i] + " ord=" + i);
//            }
            visitor.visit(docIds[i]);
        }
        //System.out.println("=================================================");
    }

    private void readSortedWithOrder(IndexInput in, int count, int[] docIds) throws IOException {
        byte valueFormat = in.readByte();
        int[] sortedValues = new int[count];

        switch (valueFormat) {
            case CONTINUOUS_IDS:
                int start = in.readVInt();
                for (int i = 0; i < count; i++) {
                    sortedValues[i] = start + i;
                }
                break;

            case SORTED_DELTA:
                int deltaCount = in.readVInt();
                if (deltaCount != count) {
                    throw new IOException("Count mismatch");
                }

                sortedValues[0] = in.readVInt();
                byte bitsPerValue = in.readByte();

                PackedInts.ReaderIterator deltaIt = PackedInts.getReaderIteratorNoHeader(in, PackedInts.Format.PACKED, PackedInts.VERSION_CURRENT, count - 1, bitsPerValue, PackedInts.DEFAULT_BUFFER_SIZE);
                for (int i = 1; i < count; i++) {
                    sortedValues[i] = sortedValues[i - 1] + (int) deltaIt.next();
                }
                break;

            default:
                throw new IOException("Unknown value format: " + valueFormat);
        }
        // Read positions
        byte posBitsPerValue = in.readByte();
        PackedInts.ReaderIterator posIt = PackedInts.getReaderIteratorNoHeader(in, PackedInts.Format.PACKED, PackedInts.VERSION_CURRENT, count, posBitsPerValue, PackedInts.DEFAULT_BUFFER_SIZE);

        for (int i = 0; i < count; i++) {
            int position = (int) posIt.next();
            //System.out.println(sortedValues[position] + "===" + docIds[i] + "===" + i);
            docIds[i] = sortedValues[position];
        }

        // Verify order and visit docIds
//        for (int i = 0; i < count; i++) {
//            if (i > 0 && docIds[i] <= docIds[i - 1]) {
//                throw new AssertionError("docs out of order: last doc=" + docIds[i-1] + " current doc=" + docIds[i] + " ord=" + i);
//            }
//        }
    }


    private static void writeIdsAsBitSet(int[] docIds, int start, int count, DataOutput out)
            throws IOException {
        int min = docIds[start];
        int max = docIds[start + count - 1];

        final int offsetWords = min >> 6;
        final int offsetBits = offsetWords << 6;
        final int totalWordCount = FixedBitSet.bits2words(max - offsetBits + 1);
        long currentWord = 0;
        int currentWordIndex = 0;

        out.writeVInt(offsetWords);
        out.writeVInt(totalWordCount);
        // build bit set streaming
        for (int i = 0; i < count; i++) {
            final int index = docIds[start + i] - offsetBits;
            final int nextWordIndex = index >> 6;
            assert currentWordIndex <= nextWordIndex;
            if (currentWordIndex < nextWordIndex) {
                out.writeLong(currentWord);
                currentWord = 0L;
                currentWordIndex++;
                while (currentWordIndex < nextWordIndex) {
                    currentWordIndex++;
                    out.writeLong(0L);
                }
            }
            currentWord |= 1L << index;
        }
        out.writeLong(currentWord);
        assert currentWordIndex + 1 == totalWordCount;
    }

    /**
     * Read {@code count} integers into {@code docIDs}.
     */
    void readInts(IndexInput in, int count, int[] docIDs) throws IOException {
        final int bpv = in.readByte();
        switch (bpv) {
            case SORTED_WITH_ORDER:
                readSortedWithOrder(in, count, docIDs);
                break;
            case CONTINUOUS_IDS:
                readContinuousIds(in, count, docIDs);
                break;
            case BITSET_IDS:
                readBitSet(in, count, docIDs);
                break;
            case DELTA_BPV_16:
                readDelta16(in, count, docIDs);
                break;
            case BPV_24:
                readInts24(in, count, docIDs);
                break;
            case BPV_32:
                readInts32(in, count, docIDs);
                break;
            case LEGACY_DELTA_VINT:
                readLegacyDeltaVInts(in, count, docIDs);
                break;
            default:
                throw new IOException("Unsupported number of bits per value: " + bpv);
        }
    }

    private DocIdSetIterator readBitSetIterator(IndexInput in, int count) throws IOException {
        int offsetWords = in.readVInt();
        int longLen = in.readVInt();
        scratchLongs.longs = ArrayUtil.growNoCopy(scratchLongs.longs, longLen);
        in.readLongs(scratchLongs.longs, 0, longLen);
        // make ghost bits clear for FixedBitSet.
        if (longLen < scratchLongs.length) {
            Arrays.fill(scratchLongs.longs, longLen, scratchLongs.longs.length, 0);
        }
        scratchLongs.length = longLen;
        FixedBitSet bitSet = new FixedBitSet(scratchLongs.longs, longLen << 6);
        return new DocBaseBitSetIterator(bitSet, count, offsetWords << 6);
    }

    private static void readContinuousIds(IndexInput in, int count, int[] docIDs) throws IOException {
        int start = in.readVInt();
        for (int i = 0; i < count; i++) {
            docIDs[i] = start + i;
        }
    }

    private static void readLegacyDeltaVInts(IndexInput in, int count, int[] docIDs)
            throws IOException {
        int doc = 0;
        for (int i = 0; i < count; i++) {
            doc += in.readVInt();
            docIDs[i] = doc;
        }
    }

    private void readBitSet(IndexInput in, int count, int[] docIDs) throws IOException {
        DocIdSetIterator iterator = readBitSetIterator(in, count);
        int docId, pos = 0;
        while ((docId = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            docIDs[pos++] = docId;
        }
        assert pos == count : "pos: " + pos + ", count: " + count;
    }

    private static void readDelta16(IndexInput in, int count, int[] docIDs) throws IOException {
        final int min = in.readVInt();
        final int halfLen = count >>> 1;
        in.readInts(docIDs, 0, halfLen);
        for (int i = 0; i < halfLen; ++i) {
            int l = docIDs[i];
            docIDs[i] = (l >>> 16) + min;
            docIDs[halfLen + i] = (l & 0xFFFF) + min;
        }
        if ((count & 1) == 1) {
            docIDs[count - 1] = Short.toUnsignedInt(in.readShort()) + min;
        }
    }

    private static void readInts24(IndexInput in, int count, int[] docIDs) throws IOException {
        int i;
        for (i = 0; i < count - 7; i += 8) {
            long l1 = in.readLong();
            long l2 = in.readLong();
            long l3 = in.readLong();
            docIDs[i] = (int) (l1 >>> 40);
            docIDs[i + 1] = (int) (l1 >>> 16) & 0xffffff;
            docIDs[i + 2] = (int) (((l1 & 0xffff) << 8) | (l2 >>> 56));
            docIDs[i + 3] = (int) (l2 >>> 32) & 0xffffff;
            docIDs[i + 4] = (int) (l2 >>> 8) & 0xffffff;
            docIDs[i + 5] = (int) (((l2 & 0xff) << 16) | (l3 >>> 48));
            docIDs[i + 6] = (int) (l3 >>> 24) & 0xffffff;
            docIDs[i + 7] = (int) l3 & 0xffffff;
        }
        for (; i < count; ++i) {
            docIDs[i] = (Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte());
        }
    }

    private static void readInts32(IndexInput in, int count, int[] docIDs) throws IOException {
        in.readInts(docIDs, 0, count);
    }

    /**
     * Read {@code count} integers and feed the result directly to {@link
     * IntersectVisitor#visit(int)}.
     */
    void readInts(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
        final int bpv = in.readByte();
        switch (bpv) {
            case SORTED_WITH_ORDER:
                readSortedWithOrder(in, count, visitor);
                break;
            case CONTINUOUS_IDS:
                readContinuousIds(in, count, visitor);
                break;
            case BITSET_IDS:
                readBitSet(in, count, visitor);
                break;
            case DELTA_BPV_16:
                readDelta16(in, count, visitor);
                break;
            case BPV_24:
                readInts24(in, count, visitor);
                break;
            case BPV_32:
                readInts32(in, count, visitor);
                break;
            case LEGACY_DELTA_VINT:
                readLegacyDeltaVInts(in, count, visitor);
                break;
            default:
                throw new IOException("Unsupported number of bits per value: " + bpv);
        }
    }

    private void readBitSet(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
        DocIdSetIterator bitSetIterator = readBitSetIterator(in, count);
        visitor.visit(bitSetIterator);
    }

    private static void readContinuousIds(IndexInput in, int count, IntersectVisitor visitor)
            throws IOException {
        int start = in.readVInt();
        int extra = start & 63;
        int offset = start - extra;
        int numBits = count + extra;
        FixedBitSet bitSet = new FixedBitSet(numBits);
        bitSet.set(extra, numBits);
        visitor.visit(new DocBaseBitSetIterator(bitSet, count, offset));
    }

    private static void readLegacyDeltaVInts(IndexInput in, int count, IntersectVisitor visitor)
            throws IOException {
        int doc = 0;
        for (int i = 0; i < count; i++) {
            doc += in.readVInt();
            visitor.visit(doc);
        }
    }

    private void readDelta16(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
        readDelta16(in, count, scratch);
        scratchIntsRef.ints = scratch;
        scratchIntsRef.length = count;
        visitor.visit(scratchIntsRef);
    }

    private static void readInts24(IndexInput in, int count, IntersectVisitor visitor)
            throws IOException {
        int i;
        for (i = 0; i < count - 7; i += 8) {
            long l1 = in.readLong();
            long l2 = in.readLong();
            long l3 = in.readLong();
            visitor.visit((int) (l1 >>> 40));
            visitor.visit((int) (l1 >>> 16) & 0xffffff);
            visitor.visit((int) (((l1 & 0xffff) << 8) | (l2 >>> 56)));
            visitor.visit((int) (l2 >>> 32) & 0xffffff);
            visitor.visit((int) (l2 >>> 8) & 0xffffff);
            visitor.visit((int) (((l2 & 0xff) << 16) | (l3 >>> 48)));
            visitor.visit((int) (l3 >>> 24) & 0xffffff);
            visitor.visit((int) l3 & 0xffffff);
        }
        for (; i < count; ++i) {
            visitor.visit((Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte()));
        }
    }

    private void readInts32(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
        in.readInts(scratch, 0, count);
        scratchIntsRef.ints = scratch;
        scratchIntsRef.length = count;
        visitor.visit(scratchIntsRef);
    }
}
