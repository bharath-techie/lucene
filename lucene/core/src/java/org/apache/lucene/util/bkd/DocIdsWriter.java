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

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.DocBaseBitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LongsRef;
import org.roaringbitmap.RoaringBitmap;

final class DocIdsWriter {

  private static final byte CONTINUOUS_IDS = (byte) -2;
  private static final byte BITSET_IDS = (byte) -1;
  private static final byte DELTA_BPV_16 = (byte) 16;
  private static final byte BPV_24 = (byte) 24;
  private static final byte BPV_32 = (byte) 32;
  private static final byte ROARING_BITMAP = 6; // New format identifier
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

  DocIdsWriter(int maxPointsInLeaf) {
    scratch = new int[maxPointsInLeaf];
  }

  void writeDocIds(int[] docIds, int start, int count, DataOutput out) throws IOException {
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
// Check if Roaring Bitmap would be beneficial
    if (shouldUseRoaringBitmap(min, max, count)) {
      System.out.println("roaring bitmap");
      writeRoaringBitmap(docIds, start, count, min, max, out);
      return;
    }


    if (min2max <= 0xFFFF) {
      System.out.println("bpv 16");
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
        System.out.println("bpv 24");
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
        System.out.println("bpv 32");
        out.writeByte(BPV_32);
        for (int i = 0; i < count; i++) {
          out.writeInt(docIds[start + i]);
        }
      }
    }
  }

  private boolean shouldUseRoaringBitmap(int min, int max, int count) {
    // Heuristic to decide when to use Roaring Bitmap
    long range = (long)max - min;
    double density = count / (double)range;

    // Use Roaring Bitmap when:
    // 1. Range is large enough
    // 2. Density is neither too high nor too low
    return range > 4096 && density > 0.01 && density < 0.7;
  }

  private void writeRoaringBitmap(int[] docIds, int start, int count, int min, int max, DataOutput out) throws IOException {
    out.writeByte(ROARING_BITMAP);
    out.writeVInt(count);


    // Group by high bits (container index)
    Map<Short, BitSet> containers = new HashMap<>();

    for (int i = 0; i < count; i++) {
      int docId = docIds[start + i];
      short high = (short)(docId >>> 16);
      int low = (docId & 0xFFFF);

      containers.computeIfAbsent(high, k -> new BitSet()).set(low);
    }

    // Write number of containers
    out.writeVInt(containers.size());

    // Write containers in sorted order
    TreeMap<Short, BitSet> sortedContainers = new TreeMap<>(containers);
    for (Map.Entry<Short, BitSet> entry : sortedContainers.entrySet()) {
      out.writeShort(entry.getKey());
      BitSet bits = entry.getValue();

      // Write the bitset efficiently
      byte[] bytes = bits.toByteArray();
      out.writeVInt(bytes.length);
      out.writeBytes(bytes, 0, bytes.length);
    }
  }

  private static class Container {
    private final BitSet values = new BitSet(1 << 16);
    private int cardinality = 0;

    void add(int value) {
      if (!values.get(value)) {
        values.set(value);
        cardinality++;
      }
    }

    void writeTo(DataOutput out) throws IOException {
      // Choose array or bitmap representation based on cardinality
      if (cardinality < 4096) { // Array encoding
        out.writeByte((byte) 0); // Array type
        out.writeVInt(cardinality);
        for (int i = values.nextSetBit(0); i >= 0; i = values.nextSetBit(i + 1)) {
          out.writeShort((short)i);
        }
      } else { // Bitmap encoding
        out.writeByte((byte) 1); // Bitmap type
        byte[] bytes = values.toByteArray();
        out.writeVInt(bytes.length);
        out.writeBytes(bytes, 0, bytes.length);
      }
    }
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
    System.out.println("writing bitset ids");
    assert currentWordIndex + 1 == totalWordCount;
  }

  /** Read {@code count} integers into {@code docIDs}. */
  void readInts(IndexInput in, int count, int[] docIDs) throws IOException {
    final int bpv = in.readByte();
    switch (bpv) {
      case ROARING_BITMAP:
        readRoaringBitmap(in, count, docIDs);
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
      // 4096 * 3 bytes
      // 512 * 3 bytes * more blocks
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

  private void readRoaringBitmap(IndexInput in, int count,  int[] docIds) throws IOException {
    int expectedCount = in.readVInt();
    assert expectedCount == count;

    int containerCount = in.readVInt();
    int docIdIndex = 0;

    // Read containers in order (they were written in sorted order)
    for (int i = 0; i < containerCount; i++) {
      short high = in.readShort();
      int byteCount = in.readVInt();
      byte[] bytes = new byte[byteCount];
      in.readBytes(bytes, 0, byteCount);

      BitSet bits = BitSet.valueOf(bytes);

      // Process bits in order to maintain sorting
      for (int bit = bits.nextSetBit(0); bit >= 0; bit = bits.nextSetBit(bit + 1)) {
        docIds[docIdIndex++] = (high << 16) | (bit & 0xFFFF);
      }
    }

    assert docIdIndex == count : "Read " + docIdIndex + " docs, expected " + count;

    // Verify sorting (can be disabled in production)
    assert isSorted(docIds, count) : "DocIds are not properly sorted";
  }

  private static void readRoaringBitmap(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    int expectedCount = in.readVInt();
    assert expectedCount == count;
    int containerCount = in.readVInt();

    for (int i = 0; i < containerCount; i++) {
      short high = in.readShort();
      int byteCount = in.readVInt();
      byte[] bytes = new byte[byteCount];
      in.readBytes(bytes, 0, byteCount);

      BitSet bits = BitSet.valueOf(bytes);

      // Process bits in order to maintain sorting
      for (int bit = bits.nextSetBit(0); bit >= 0; bit = bits.nextSetBit(bit + 1)) {
        int docId = (high << 16) | (bit & 0xFFFF);
        visitor.visit(docId);
      }
    }
  }

  private boolean isSorted(int[] array, int count) {
    for (int i = 1; i < count; i++) {
      if (array[i - 1] >= array[i]) {
        return false;
      }
    }
    return true;
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
      case ROARING_BITMAP:
        readRoaringBitmap(in, count, visitor);
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
