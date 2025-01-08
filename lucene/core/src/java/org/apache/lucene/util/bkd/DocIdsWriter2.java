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

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

final class DocIdsWriter2 {

  private static final byte CONTINUOUS_IDS = (byte) -2;
  private static final byte BITSET_IDS = (byte) -1;
  private static final byte DELTA_BPV_16 = (byte) 16;
  private static final byte BPV_24 = (byte) 24;
  private static final byte BPV_32 = (byte) 32;
  // These signs are legacy, should no longer be used in the writing side.
  private static final byte LEGACY_DELTA_VINT = (byte) 0;
  private static final byte ROARING_BITMAP = (byte) 6; // New format identifier
  private static final byte SORTED_WITH_ORDER = (byte) 7; // New format identifier


  private static final byte SORTED_RLE = (byte) 9;
  private static final byte SORTED_DELTA = (byte) 10;
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

  DocIdsWriter2(int maxPointsInLeaf) {
    scratch = new int[maxPointsInLeaf];
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

  private void writeSortedDocIds(int[] sortedDocIds, int count, DataOutput out) throws IOException {
    // First check if it's a continuous sequence
    if (sortedDocIds[count - 1] - sortedDocIds[0] + 1 == count) {
      out.writeByte(CONTINUOUS_IDS);
      out.writeVInt(sortedDocIds[0]);
      return;
    }

    // Analyze sequence to decide between RLE and delta
    int numRuns = countRuns(sortedDocIds, count);
    if (shouldUseRLE(count, numRuns)) {
      writeRLESorted(sortedDocIds, count, out);
    } else {
      writeDeltaSorted(sortedDocIds, count, out);
    }
  }

  private int countRuns(int[] sortedDocIds, int count) {
    int runs = 1;
    for (int i = 1; i < count; i++) {
      if (sortedDocIds[i] != sortedDocIds[i-1] + 1) {
        runs++;
      }
    }
    return runs;
  }

  private boolean shouldUseRLE(int count, int numRuns) {
    // Use RLE if average run length is >= 4
    return (count / numRuns) >= 4;
  }

  private void writeRLESorted(int[] sortedDocIds, int count, DataOutput out) throws IOException {
    out.writeByte(SORTED_RLE);
    out.writeVInt(count);  // Write count first
    out.writeVInt(sortedDocIds[0]);  // Write first value

    int pos = 1;
    while (pos < count) {
      int runStart = pos;
      while (pos < count && sortedDocIds[pos] == sortedDocIds[pos-1] + 1) {
        pos++;
      }
      int runLength = pos - runStart;
      out.writeVInt(runLength);

      if (pos < count) {
        out.writeVInt(sortedDocIds[pos++]);
      }
    }
  }

  private void writeDeltaSorted(int[] sortedDocIds, int count, DataOutput out) throws IOException {
    out.writeByte(SORTED_DELTA);
    out.writeVInt(count);  // Write count first
    out.writeVInt(sortedDocIds[0]);  // Write first value

    // Calculate and write deltas
    int maxDelta = 0;
    for (int i = 1; i < count; i++) {
      int delta = sortedDocIds[i] - sortedDocIds[i-1];
      scratch[i-1] = delta;
      maxDelta = Math.max(maxDelta, delta);
    }

    // Choose and write BPV
    byte bpv;
    if (maxDelta <= 0xFFFF) {
      bpv = 16;
    } else if (maxDelta <= 0xFFFFFF) {
      bpv = 24;
    } else {
      bpv = 32;
    }
    out.writeByte(bpv);

    // Write deltas
    switch (bpv) {
      case 16:
        writeDeltasBPV16(scratch, count-1, out);
        break;
      case 24:
        writeDeltasBPV24(scratch, count-1, out);
        break;
      case 32:
        writeDeltasBPV32(scratch, count-1, out);
        break;
    }
  }

  private void readSortedDocIds(IndexInput in, int count, int[] docIds) throws IOException {
    // Read format byte
    byte format = in.readByte();

    switch (format) {
      case CONTINUOUS_IDS:
        // Handle continuous sequence
        int start = in.readVInt();
        for (int i = 0; i < count; i++) {
          docIds[i] = start + i;
        }
        break;

      case SORTED_RLE:
        readRLESorted(in, count, docIds);
        break;

      case SORTED_DELTA:
        readDeltaSorted(in, count, docIds);
        break;

      default:
        throw new IOException("Unknown sorted docIds format: " + format);
    }
  }

  private void readRLESorted(IndexInput in, int count, int[] docIds) throws IOException {
    int pos = 0;
    // Read first value
    docIds[pos++] = in.readVInt();

    while (pos < count) {
      int runLength = in.readVInt();
      // Expand run
      for (int i = 0; i < runLength; i++) {
        docIds[pos] = docIds[pos - 1] + 1;
        pos++;
      }
      // Read next value if not at end
      if (pos < count) {
        docIds[pos++] = in.readVInt();
      }
    }
  }

  private void readDeltaSorted(IndexInput in, int count, int[] docIds) throws IOException {
    // Read first value
    docIds[0] = in.readVInt();

    // Read BPV for deltas
    byte bpv = in.readByte();

    // Read deltas into scratch array
    switch (bpv) {
      case 16:
        readDeltasBPV16(in, count - 1, scratch);
        break;
      case 24:
        readDeltasBPV24(in, count - 1, scratch);
        break;
      case 32:
        readDeltasBPV32(in, count - 1, scratch);
        break;
      default:
        throw new IOException("Invalid BPV for delta encoding: " + bpv);
    }

    // Reconstruct values from deltas
    for (int i = 1; i < count; i++) {
      docIds[i] = docIds[i - 1] + scratch[i - 1];
    }
  }

  // And the visitor version:
  private void readSortedDocIds(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    byte format = in.readByte();

    switch (format) {
      case CONTINUOUS_IDS:
        int start = in.readVInt();
        for (int i = 0; i < count; i++) {
          visitor.visit(start + i);
        }
        break;

      case SORTED_RLE:
        readRLESortedVisitor(in, count, visitor);
        break;

      case SORTED_DELTA:
        readDeltaSortedVisitor(in, count, visitor);
        break;

      default:
        throw new IOException("Unknown sorted docIds format: " + format);
    }
  }

  private void readRLESortedVisitor(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    int current = in.readVInt();
    visitor.visit(current);
    int pos = 1;

    while (pos < count) {
      int runLength = in.readVInt();
      // Visit run
      for (int i = 0; i < runLength; i++) {
        current++;
        visitor.visit(current);
      }
      pos += runLength;

      // Read and visit next value if not at end
      if (pos < count) {
        current = in.readVInt();
        visitor.visit(current);
        pos++;
      }
    }
  }

  private void readDeltaSortedVisitor(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    int current = in.readVInt();
    visitor.visit(current);

    byte bpv = in.readByte();
    switch (bpv) {
      case 16:
        readDeltasBPV16Visitor(in, count - 1, current, visitor);
        break;
      case 24:
        readDeltasBPV24Visitor(in, count - 1, current, visitor);
        break;
      case 32:
        readDeltasBPV32Visitor(in, count - 1, current, visitor);
        break;
      default:
        throw new IOException("Invalid BPV for delta encoding: " + bpv);
    }
  }

  private void readDeltasBPV16(IndexInput in, int count, int[] deltas) throws IOException {
    final int halfLen = count >>> 1;
    // Read packed values
    for (int i = 0; i < halfLen; i++) {
      int packed = in.readInt();
      deltas[i] = packed >>> 16;          // Upper 16 bits
      deltas[halfLen + i] = packed & 0xFFFF;  // Lower 16 bits
    }
    // Handle odd count
    if ((count & 1) == 1) {
      deltas[count - 1] = Short.toUnsignedInt(in.readShort());
    }
  }

  private void readDeltasBPV24(IndexInput in, int count, int[] deltas) throws IOException {
    int i;
    for (i = 0; i < count - 7; i += 8) {
      long l1 = in.readLong();
      long l2 = in.readLong();
      long l3 = in.readLong();

      deltas[i] = (int) (l1 >>> 40);
      deltas[i + 1] = (int) (l1 >>> 16) & 0xffffff;
      deltas[i + 2] = (int) (((l1 & 0xffff) << 8) | (l2 >>> 56));
      deltas[i + 3] = (int) (l2 >>> 32) & 0xffffff;
      deltas[i + 4] = (int) (l2 >>> 8) & 0xffffff;
      deltas[i + 5] = (int) (((l2 & 0xff) << 16) | (l3 >>> 48));
      deltas[i + 6] = (int) (l3 >>> 24) & 0xffffff;
      deltas[i + 7] = (int) l3 & 0xffffff;
    }
    // Handle remaining values
    for (; i < count; ++i) {
      deltas[i] = (Short.toUnsignedInt(in.readShort()) << 8) |
              Byte.toUnsignedInt(in.readByte());
    }
  }

  private void readDeltasBPV32(IndexInput in, int count, int[] deltas) throws IOException {
    in.readInts(deltas, 0, count);
  }

  // And here's the visitor version if needed:
  private void readDeltasSorted(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    int current = in.readVInt(); // First value
    visitor.visit(current);

    byte bpv = in.readByte();
    switch (bpv) {
      case 16:
        readDeltasBPV16Visitor(in, count-1, current, visitor);
        break;
      case 24:
        readDeltasBPV24Visitor(in, count-1, current, visitor);
        break;
      case 32:
        readDeltasBPV32Visitor(in, count-1, current, visitor);
        break;
      default:
        throw new IOException("Invalid BPV for delta encoding: " + bpv);
    }
  }

  private void readDeltasBPV16Visitor(IndexInput in, int count, int current, IntersectVisitor visitor)
          throws IOException {
    final int halfLen = count >>> 1;

    // Process pairs of deltas
    for (int i = 0; i < halfLen; i++) {
      int packed = in.readInt();
      current += packed >>> 16;          // First delta
      visitor.visit(current);
      current += packed & 0xFFFF;        // Second delta
      visitor.visit(current);
    }

    // Handle odd count
    if ((count & 1) == 1) {
      current += Short.toUnsignedInt(in.readShort());
      visitor.visit(current);
    }
  }

  private void readDeltasBPV24Visitor(IndexInput in, int count, int current, IntersectVisitor visitor)
          throws IOException {
    int i;
    for (i = 0; i < count - 7; i += 8) {
      long l1 = in.readLong();
      long l2 = in.readLong();
      long l3 = in.readLong();

      current += (int) (l1 >>> 40);
      visitor.visit(current);
      current += (int) (l1 >>> 16) & 0xffffff;
      visitor.visit(current);
      current += (int) (((l1 & 0xffff) << 8) | (l2 >>> 56));
      visitor.visit(current);
      current += (int) (l2 >>> 32) & 0xffffff;
      visitor.visit(current);
      current += (int) (l2 >>> 8) & 0xffffff;
      visitor.visit(current);
      current += (int) (((l2 & 0xff) << 16) | (l3 >>> 48));
      visitor.visit(current);
      current += (int) (l3 >>> 24) & 0xffffff;
      visitor.visit(current);
      current += (int) l3 & 0xffffff;
      visitor.visit(current);
    }

    // Handle remaining values
    for (; i < count; ++i) {
      current += (Short.toUnsignedInt(in.readShort()) << 8) |
              Byte.toUnsignedInt(in.readByte());
      visitor.visit(current);
    }
  }

  private void readDeltasBPV32Visitor(IndexInput in, int count, int current, IntersectVisitor visitor)
          throws IOException {
    for (int i = 0; i < count; i++) {
      current += in.readInt();
      visitor.visit(current);
    }
  }

  // Helper methods for BPV delta encoding/decoding
  private void writeDeltasBPV16(int[] deltas, int count, DataOutput out) throws IOException {
    final int halfLen = count >>> 1;

    for (int i = 0; i < halfLen; ++i) {
      scratch[i] = deltas[halfLen + i] | (deltas[i] << 16);
    }
    for (int i = 0; i < halfLen; i++) {
      out.writeInt(scratch[i]);
    }
    if ((count & 1) == 1) {
      out.writeShort((short) deltas[count - 1]);
    }
  }

  private void writeDeltasBPV24(int[] deltas, int count, DataOutput out) throws IOException {
    int i;
    // Process groups of 8 deltas at a time
    for (i = 0; i < count - 7; i += 8) {
      int delta1 = deltas[i];
      int delta2 = deltas[i + 1];
      int delta3 = deltas[i + 2];
      int delta4 = deltas[i + 3];
      int delta5 = deltas[i + 4];
      int delta6 = deltas[i + 5];
      int delta7 = deltas[i + 6];
      int delta8 = deltas[i + 7];

      // Pack 8 24-bit deltas into 3 longs
      long l1 = ((delta1 & 0xffffffL) << 40) |
              ((delta2 & 0xffffffL) << 16) |
              ((long)(delta3 >>> 8) & 0xffffL);

      long l2 = ((delta3 & 0xffL) << 56) |
              ((delta4 & 0xffffffL) << 32) |
              ((delta5 & 0xffffffL) << 8) |
              ((long)(delta6 >>> 16) & 0xffL);

      long l3 = ((delta6 & 0xffffL) << 48) |
              ((delta7 & 0xffffffL) << 24) |
              (delta8 & 0xffffffL);

      out.writeLong(l1);
      out.writeLong(l2);
      out.writeLong(l3);
    }

    // Handle remaining deltas
    for (; i < count; ++i) {
      out.writeShort((short) (deltas[i] >>> 8));
      out.writeByte((byte) deltas[i]);
    }
  }

  private void writeDeltasBPV32(int[] deltas, int count, DataOutput out) throws IOException {
    for (int i = 0; i < count; i++) {
      out.writeInt(deltas[i]);
    }
  }

  void writeDocIds(int[] docIds, int start, int count, DataOutput out) throws IOException {
    // docs can be sorted either when all docs in a block have the same value
    // or when a segment is sorted

    // Check if this would be a good case for using sorted with order format
    //System.out.println("count"+count);
    boolean useSortedWithOrder = shouldUseSortedWithOrder(docIds, start, count);

    if (useSortedWithOrder) {
      //System.out.println("write sorted");
      writeSortedWithOrder(docIds, start, count, out);
      return;
    }
    System.out.println("not write sorted");

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
      out.writeByte(SORTED_DELTA);
      out.writeVInt(count);
      out.writeVInt(sortedDocIds[0]);

      // Calculate and write deltas
      int maxDelta = 0;
      for (int i = 1; i < count; i++) {
        int delta = sortedDocIds[i] - sortedDocIds[i-1];
        scratch[i-1] = delta;
        maxDelta = Math.max(maxDelta, delta);
      }

      byte bpv;
      if (maxDelta <= 0xFFFF) {
        bpv = 16;
      } else if (maxDelta <= 0xFFFFFF) {
        bpv = 24;
      } else {
        bpv = 32;
      }
      out.writeByte(bpv);

      switch (bpv) {
        case 16:
          writeDeltasBPV16(scratch, count-1, out);
          break;
        case 24:
          writeDeltasBPV24(scratch, count-1, out);
          break;
        case 32:
          writeDeltasBPV32(scratch, count-1, out);
          break;
      }
    }

    // Write inverse mapping (position -> index)
    int[] inverseMapping = new int[count];
    for (int i = 0; i < count; i++) {
      inverseMapping[indexedValues[i].originalIndex] = i;
    }

    // Write positions
    int bitsPerPosition = 32 - Integer.numberOfLeadingZeros(count - 1);
    int numBytes = (count * bitsPerPosition + 7) / 8;
    byte[] packedPositions = new byte[numBytes];

    for (int i = 0; i < count; i++) {
      writeBits(packedPositions, i * bitsPerPosition, inverseMapping[i], bitsPerPosition);
    }

    out.writeByte((byte) bitsPerPosition);
    out.writeInt(numBytes);
    out.writeBytes(packedPositions, 0, numBytes);
  }

  private static void writeBits(byte[] array, int bitOffset, int value, int numBits) {
    int byteOffset = bitOffset / 8;
    int bitInByte = bitOffset % 8;

    // Write value across byte boundaries as needed
    value &= (1 << numBits) - 1; // Mask to required bits
    value <<= bitInByte;

    for (int i = 0; i < ((numBits + bitInByte + 7) / 8); i++) {
      if (byteOffset + i < array.length) {
        array[byteOffset + i] |= (byte)(value >>> (i * 8));
      }
    }
  }

  private void readSortedWithOrder(IndexInput in, int count, int[] docIds) throws IOException {
//    byte format = in.readByte();
//    if (format != SORTED_WITH_ORDER) {
//      throw new IOException("Expected SORTED_WITH_ORDER format, but got: " + format);
//    }

    // Read sorted values into scratch array
    byte valueFormat = in.readByte();
    switch (valueFormat) {
      case CONTINUOUS_IDS:
        int start = in.readVInt();
        for (int i = 0; i < count; i++) {
          scratch[i] = start + i;
        }
        break;

      case SORTED_DELTA:
        int deltaCount = in.readVInt();
        if (deltaCount != count) {
          throw new IOException("Count mismatch: expected " + count + " but got " + deltaCount);
        }

        scratch[0] = in.readVInt();
        byte bpv = in.readByte();
        if (bpv != 16 && bpv != 24 && bpv != 32) {
          throw new IOException("Invalid BPV for delta encoding: " + bpv);
        }

        // Read deltas and reconstruct values
        int[] deltas = new int[count - 1];
        switch (bpv) {
          case 16:
            readDeltasBPV16(in, count - 1, deltas);
            break;
          case 24:
            readDeltasBPV24(in, count - 1, deltas);
            break;
          case 32:
            readDeltasBPV32(in, count - 1, deltas);
            break;
        }

        // Reconstruct values
        for (int i = 1; i < count; i++) {
          scratch[i] = scratch[i - 1] + deltas[i - 1];
        }
        break;

      default:
        throw new IOException("Unknown value format: " + valueFormat);
    }

    // Read position metadata
    int bitsPerPosition = in.readByte() & 0xFF;
    int numBytes = in.readInt();
    byte[] packedPositions = new byte[numBytes];
    in.readBytes(packedPositions, 0, numBytes);

    // Create mapping array
    int[] mapping = new int[count];
    for (int i = 0; i < count; i++) {
      mapping[i] = readBits(packedPositions, i * bitsPerPosition, bitsPerPosition);
    }

    // Apply mapping to get final docIds in sorted order
    for (int i = 0; i < count; i++) {
      docIds[i] = scratch[mapping[i]];
    }

    // Verify order
//    for (int i = 1; i < count; i++) {
//      if (docIds[i] <= docIds[i-1]) {
//        throw new AssertionError("docs out of order: last doc=" + docIds[i-1] +
//                " current doc=" + docIds[i] + " ord=" + i);
//      }
//    }
  }

  // Optimized readBits method that works with smaller chunks
  private static int readBits(byte[] array, int bitOffset, int numBits) {
    int byteOffset = bitOffset >>> 3;
    int bitInByte = bitOffset & 7;

    // Fast path for common cases
    if (numBits <= 8 && bitInByte + numBits <= 8) {
      return (array[byteOffset] >>> bitInByte) & ((1 << numBits) - 1);
    }

    int result = 0;
    int bitsRemaining = numBits;

    // First byte
    int firstByte = array[byteOffset] & 0xFF;
    int availableBits = 8 - bitInByte;
    int bitsToTake = Math.min(availableBits, bitsRemaining);
    result = (firstByte >>> bitInByte) & ((1 << bitsToTake) - 1);
    bitsRemaining -= bitsToTake;

    // Middle bytes
    while (bitsRemaining >= 8) {
      result |= (array[++byteOffset] & 0xFF) << (numBits - bitsRemaining);
      bitsRemaining -= 8;
    }

    // Last byte if any bits remaining
    if (bitsRemaining > 0) {
      result |= (array[++byteOffset] & ((1 << bitsRemaining) - 1)) << (numBits - bitsRemaining);
    }

    return result;
  }

  private void readSortedWithOrder(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    // Read sorted docIds format
    final byte format = in.readByte();

    // Read sorted docIds into scratch array
    switch (format) {
      case CONTINUOUS_IDS:
        int start = in.readVInt();
        for (int i = 0; i < count; i++) {
          scratch[i] = start + i;
        }
        break;

      case SORTED_RLE:
        readRLESorted(in, count, scratch);
        break;

      case SORTED_DELTA:
        readDeltaSorted(in, count, scratch);
        break;

      default:
        throw new IOException("Unknown sorted docIds format: " + format);
    }

    // Read position metadata
    int bitsPerPosition = in.readByte() & 0xFF;
    int numBytes = in.readInt();

    // Process in chunks for better memory efficiency
    final int CHUNK_SIZE = 1024;
    byte[] packedPositions = new byte[Math.min(numBytes, (CHUNK_SIZE * bitsPerPosition + 7) / 8)];
    int[] reorderedDocs = new int[count]; // Temporary array for reordering

    int processedCount = 0;
    while (processedCount < count) {
      int chunkSize = Math.min(CHUNK_SIZE, count - processedCount);
      int bytesNeeded = (chunkSize * bitsPerPosition + 7) / 8;

      if (packedPositions.length < bytesNeeded) {
        packedPositions = new byte[bytesNeeded];
      }
      in.readBytes(packedPositions, 0, bytesNeeded);

      // Process chunk
      for (int i = 0; i < chunkSize; i++) {
        int position = readBits(packedPositions, i * bitsPerPosition, bitsPerPosition);
        reorderedDocs[position] = scratch[processedCount + i];
      }

      processedCount += chunkSize;
    }

    // Skip any remaining bytes
    int remainingBytes = numBytes - ((count * bitsPerPosition + 7) / 8);
    if (remainingBytes > 0) {
      in.skipBytes(remainingBytes);
    }

    // Visit using reordered docs
    scratchIntsRef.ints = reorderedDocs;
    scratchIntsRef.length = count;
    visitor.visit(scratchIntsRef);
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

  /** Read {@code count} integers into {@code docIDs}. */
  void readInts(IndexInput in, int count, int[] docIDs) throws IOException {
    final int bpv = in.readByte();
    switch (bpv) {
      case SORTED_WITH_ORDER:
        readSortedWithOrder(in, count, docIDs);
        break;
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
