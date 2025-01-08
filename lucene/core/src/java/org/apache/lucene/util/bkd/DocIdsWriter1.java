//package org.apache.lucene.util.bkd;
//
//import org.apache.lucene.index.PointValues.IntersectVisitor;
//import org.apache.lucene.search.DocIdSetIterator;
//import org.apache.lucene.store.DataOutput;
//import org.apache.lucene.store.IndexInput;
//import org.apache.lucene.util.ArrayUtil;
//import org.apache.lucene.util.DocBaseBitSetIterator;
//import org.apache.lucene.util.FixedBitSet;
//import org.apache.lucene.util.IntsRef;
//import org.apache.lucene.util.LongsRef;
//
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.BitSet;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.TreeMap;
//
//final class DocIdsWriter1 {
//
//  private static final byte CONTINUOUS_IDS = (byte) -2;
//  private static final byte BITSET_IDS = (byte) -1;
//  private static final byte DELTA_BPV_16 = (byte) 16;
//  private static final byte BPV_24 = (byte) 24;
//  private static final byte BPV_32 = (byte) 32;
//  // These signs are legacy, should no longer be used in the writing side.
//  private static final byte LEGACY_DELTA_VINT = (byte) 0;
//  private static final byte ROARING_BITMAP = (byte) 6; // New format identifier
//  private static final byte SORTED_WITH_ORDER = (byte) 7; // New format identifier
//
//  private final int[] scratch;
//  private final LongsRef scratchLongs = new LongsRef();
//
//  /**
//   * IntsRef to be used to iterate over the scratch buffer. A single instance is reused to avoid
//   * re-allocating the object. The ints and length fields need to be reset each use.
//   *
//   * <p>The main reason for existing is to be able to call the {@link
//   * IntersectVisitor#visit(IntsRef)} method rather than the {@link IntersectVisitor#visit(int)}
//   * method. This seems to make a difference in performance, probably due to fewer virtual calls
//   * then happening (once per read call rather than once per doc).
//   */
//  private final IntsRef scratchIntsRef = new IntsRef();
//
//  {
//    // This is here to not rely on the default constructor of IntsRef to set offset to 0
//    scratchIntsRef.offset = 0;
//  }
//
//  DocIdsWriter1(int maxPointsInLeaf) {
//    scratch = new int[maxPointsInLeaf];
//  }
//
//  private boolean shouldUseSortedWithOrder(int[] docIds, int start, int count) {
//    // Heuristic to decide when to use sorted with order format
//    // For example, when the range of values is large but the count is relatively small
//    int min = docIds[start];
//    int max = min;
//    for (int i = 1; i < count; i++) {
//      int val = docIds[start + i];
//      min = Math.min(min, val);
//      max = Math.max(max, val);
//    }
//
//    // Use this format when the range is large but count is small
//    return (max - min) > count * 4;
//  }
//
//  private boolean shouldUseSortedWithOrder1(int[] docIds, int start, int count) {
//    // Quick range check
//    //if (count < 128 || count > 1024) return false;
//
//    // Check first few values to detect if already sorted
//    boolean likelySorted = true;
//    for (int i = start + 1; i < Math.min(start + 16, start + count); i++) {
//      if (docIds[i] <= docIds[i-1]) {
//        likelySorted = false;
//        break;
//      }
//    }
//    if (likelySorted) return false;
//
//    // Quick min/max scan
//    int min = docIds[start];
//    int max = min;
//    for (int i = start + 1; i < start + count; i++) {
//      int val = docIds[i];
//      if (val < min) min = val;
//      if (val > max) max = val;
//    }
//    System.out.println("max-min : " + (max-min) + " , count : " + count + " , max =" + max + ", min =" + min);
//
//    // Use when range is large relative to count
//    return (max - min) > (count * 8);
//  }
//
//
//  // New method to write sorted docIds directly
//  private void writeSortedDocIds(int[] sortedDocIds, int count, DataOutput out) throws IOException {
//    // Since array is sorted, we know first is min and last is max
//    int min = sortedDocIds[0];
//    int max = sortedDocIds[count - 1];
//
//    int range = max - min + 1;
//
//    if (range == count) {
//      // Continuous ids case
//      out.writeByte(CONTINUOUS_IDS);
//      out.writeVInt(min);
//      return;
//    }
//
//    if (range <= (count << 4)) {
//      // BitSet case might be efficient
//      out.writeByte(BITSET_IDS);
//      writeIdsAsBitSet(sortedDocIds, 0, count, out);
//      return;
//    }
//
//    if (max <= 0xFFFF) {
//      // Can fit in 16 bits
//      out.writeByte(DELTA_BPV_16);
//      out.writeVInt(min);
//      final int halfLen = count >>> 1;
//      for (int i = 0; i < halfLen; ++i) {
//        scratch[i] = (sortedDocIds[halfLen + i] - min) | ((sortedDocIds[i] - min) << 16);
//      }
//      for (int i = 0; i < halfLen; i++) {
//        out.writeInt(scratch[i]);
//      }
//      if ((count & 1) == 1) {
//        out.writeShort((short) (sortedDocIds[count - 1] - min));
//      }
//    } else if (max <= 0xFFFFFF) {
//      // Can fit in 24 bits
//      out.writeByte(BPV_24);
//      int i;
//      for (i = 0; i < count - 7; i += 8) {
//        long l1 = (sortedDocIds[i] & 0xffffffL) << 40 |
//                (sortedDocIds[i + 1] & 0xffffffL) << 16 |
//                ((sortedDocIds[i + 2] >>> 8) & 0xffffL);
//        long l2 = (sortedDocIds[i + 2] & 0xffL) << 56 |
//                (sortedDocIds[i + 3] & 0xffffffL) << 32 |
//                (sortedDocIds[i + 4] & 0xffffffL) << 8 |
//                ((sortedDocIds[i + 5] >> 16) & 0xffL);
//        long l3 = (sortedDocIds[i + 5] & 0xffffL) << 48 |
//                (sortedDocIds[i + 6] & 0xffffffL) << 24 |
//                (sortedDocIds[i + 7] & 0xffffffL);
//        out.writeLong(l1);
//        out.writeLong(l2);
//        out.writeLong(l3);
//      }
//      for (; i < count; ++i) {
//        out.writeShort((short) (sortedDocIds[i] >>> 8));
//        out.writeByte((byte) sortedDocIds[i]);
//      }
//    } else {
//      // Need full 32 bits
//      out.writeByte(BPV_32);
//      for (int i = 0; i < count; i++) {
//        out.writeInt(sortedDocIds[i]);
//      }
//    }
//  }
//
//  private void writeSortedWithOrder1(int[] docIds, int start, int count, DataOutput out) throws IOException {
//    out.writeByte(SORTED_WITH_ORDER);
//
//    // Create and sort IndexedValues
//    IndexedValue[] indexedValues = new IndexedValue[count];
//    for (int i = 0; i < count; i++) {
//      indexedValues[i] = new IndexedValue(docIds[start + i], i);
//    }
//    Arrays.sort(indexedValues);
//
//    // Write sorted docIds
//    int[] sortedDocIds = new int[count];
//    for(int i = 0; i < count; i++) {
//      sortedDocIds[i] = indexedValues[i].value;
//    }
//    writeSortedDocIds(sortedDocIds, count, out);
//
//    // Pack positions more efficiently using blocks
//    int bitsPerPosition = 32 - Integer.numberOfLeadingZeros(count - 1);
//
//    // Use block-based packing for positions
//    final int BLOCK_SIZE = 32; // or 64 depending on performance testing
//    int numBlocks = (count + BLOCK_SIZE - 1) / BLOCK_SIZE;
//
//    out.writeByte((byte)bitsPerPosition);
//    out.writeVInt(numBlocks);
//
//    for (int block = 0; block < numBlocks; block++) {
//      int blockStart = block * BLOCK_SIZE;
//      int blockEnd = Math.min(blockStart + BLOCK_SIZE, count);
//
//      // Pack positions for this block
//      long packed = 0;
//      for (int i = blockStart; i < blockEnd; i++) {
//        packed |= ((long)indexedValues[i].originalIndex) << ((i - blockStart) * bitsPerPosition);
//      }
//      out.writeLong(packed);
//    }
//  }
//
//  void writeDocIds(int[] docIds, int start, int count, DataOutput out) throws IOException {
//    // docs can be sorted either when all docs in a block have the same value
//    // or when a segment is sorted
//
//    // Check if this would be a good case for using sorted with order format
//    //System.out.println("count"+count);
//    boolean useSortedWithOrder = shouldUseSortedWithOrder1(docIds, start, count);
//
//    if (useSortedWithOrder) {
//      System.out.println("write sorted");
//      writeSortedWithOrder1(docIds, start, count, out);
//      return;
//    }
//    System.out.println("not write sorted");
//
//    boolean strictlySorted = true;
//    int min = docIds[0];
//    int max = docIds[0];
//    for (int i = 1; i < count; ++i) {
//      int last = docIds[start + i - 1];
//      int current = docIds[start + i];
//      if (last >= current) {
//        strictlySorted = false;
//      }
//      min = Math.min(min, current);
//      max = Math.max(max, current);
//    }
//
//    int min2max = max - min + 1;
//    if (strictlySorted) {
//      if (min2max == count) {
//        // continuous ids, typically happens when segment is sorted
//        out.writeByte(CONTINUOUS_IDS);
//        out.writeVInt(docIds[start]);
//        return;
//      } else if (min2max <= (count << 4)) {
//        assert min2max > count : "min2max: " + min2max + ", count: " + count;
//        // Only trigger bitset optimization when max - min + 1 <= 16 * count in order to avoid
//        // expanding too much storage.
//        // A field with lower cardinality will have higher probability to trigger this optimization.
//        out.writeByte(BITSET_IDS);
//        writeIdsAsBitSet(docIds, start, count, out);
//        return;
//      }
//    }
//
//    if (min2max <= 0xFFFF) {
//      out.writeByte(DELTA_BPV_16);
//      for (int i = 0; i < count; i++) {
//        scratch[i] = docIds[start + i] - min;
//      }
//      out.writeVInt(min);
//      final int halfLen = count >>> 1;
//      for (int i = 0; i < halfLen; ++i) {
//        scratch[i] = scratch[halfLen + i] | (scratch[i] << 16);
//      }
//      for (int i = 0; i < halfLen; i++) {
//        out.writeInt(scratch[i]);
//      }
//      if ((count & 1) == 1) {
//        out.writeShort((short) scratch[count - 1]);
//      }
//    } else {
//      if (max <= 0xFFFFFF) {
//        out.writeByte(BPV_24);
//        // write them the same way we are reading them.
//        int i;
//        for (i = 0; i < count - 7; i += 8) {
//          int doc1 = docIds[start + i];
//          int doc2 = docIds[start + i + 1];
//          int doc3 = docIds[start + i + 2];
//          int doc4 = docIds[start + i + 3];
//          int doc5 = docIds[start + i + 4];
//          int doc6 = docIds[start + i + 5];
//          int doc7 = docIds[start + i + 6];
//          int doc8 = docIds[start + i + 7];
//          long l1 = (doc1 & 0xffffffL) << 40 | (doc2 & 0xffffffL) << 16 | ((doc3 >>> 8) & 0xffffL);
//          long l2 =
//              (doc3 & 0xffL) << 56
//                  | (doc4 & 0xffffffL) << 32
//                  | (doc5 & 0xffffffL) << 8
//                  | ((doc6 >> 16) & 0xffL);
//          long l3 = (doc6 & 0xffffL) << 48 | (doc7 & 0xffffffL) << 24 | (doc8 & 0xffffffL);
//          out.writeLong(l1);
//          out.writeLong(l2);
//          out.writeLong(l3);
//        }
//        for (; i < count; ++i) {
//          out.writeShort((short) (docIds[start + i] >>> 8));
//          out.writeByte((byte) docIds[start + i]);
//        }
//      } else {
//        out.writeByte(BPV_32);
//        for (int i = 0; i < count; i++) {
//          out.writeInt(docIds[start + i]);
//        }
//      }
//    }
//  }
//
//  class IndexedValue implements Comparable<IndexedValue> {
//    int value;
//    int originalIndex;
//
//    IndexedValue(int value, int originalIndex) {
//      this.value = value;
//      this.originalIndex = originalIndex;
//    }
//
//    @Override
//    public int compareTo(IndexedValue other) {
//      return Integer.compare(this.value, other.value);
//    }
//  }
//
//
//  private void writeSortedWithOrder(int[] docIds, int start, int count, DataOutput out) throws IOException {
//    //System.out.println("writing sorted");
//    out.writeByte(SORTED_WITH_ORDER);
//
//    // Create array of IndexedValue objects
//    IndexedValue[] indexedValues = new IndexedValue[count];
//    for (int i = 0; i < count; i++) {
//      indexedValues[i] = new IndexedValue(docIds[start + i], i);
//    }
//    // Sort the array
//    Arrays.sort(indexedValues);
//
//    int[] sortedDocIds = new int[indexedValues.length];
//    for(int i = 0; i < indexedValues.length; i++) {
//      sortedDocIds[i] = indexedValues[i].value;
//    }
//
//    int[] positions = new int[indexedValues.length];
//    for(int i = 0; i < indexedValues.length; i++) {
//      positions[i] = indexedValues[i].originalIndex;
//    }
//
//    // Write the sorted docIds
//    writeSortedDocIds(sortedDocIds, count, out);
//
//    // Pack positions into bytes
//    int bitsPerPosition = 32 - Integer.numberOfLeadingZeros(count - 1);
//    int totalBits = count * bitsPerPosition;
//    int numBytes = (totalBits + 7) / 8;
//    byte[] packedPositions = new byte[numBytes];
//    Arrays.fill(packedPositions, (byte) 0); // Initialize array with zeros
//
//    // Pack each position into the byte array
//    for (int i = 0; i < count; i++) {
//      int position = positions[i];
//      writeBits(packedPositions, i * bitsPerPosition, position, bitsPerPosition);
//    }
//
//    // Write number of bits per position and packed positions
//    out.writeByte((byte) bitsPerPosition);
//    out.writeInt(numBytes); // Write length of packed positions
//    out.writeBytes(packedPositions, 0, numBytes);
//  }
//
//  private static void writeBits(byte[] array, int bitOffset, int value, int numBits) {
//    int byteOffset = bitOffset / 8;
//    int bitInByte = bitOffset % 8;
//
//    // Write value across byte boundaries as needed
//    value &= (1 << numBits) - 1; // Mask to required bits
//    value <<= bitInByte;
//
//    for (int i = 0; i < ((numBits + bitInByte + 7) / 8); i++) {
//      if (byteOffset + i < array.length) {
//        array[byteOffset + i] |= (byte)(value >>> (i * 8));
//      }
//    }
//  }
//
//  private void readSortedWithOrder(IndexInput in, int count, int[] docIds) throws IOException {
//    //System.out.println("reading sorted");
//    // First read the sorted docIds using their original format
//    int[] sortedDocIds = new int[count];
//
//    // Read the format byte and handle accordingly
//    final int bpv = in.readByte();
//    switch (bpv) {
//      case CONTINUOUS_IDS:
//        readContinuousIds(in, count, sortedDocIds);
//        break;
//      case BITSET_IDS:
//        readBitSet(in, count, sortedDocIds);
//        break;
//      case DELTA_BPV_16:
//        readDelta16(in, count, sortedDocIds);
//        break;
//      case BPV_24:
//        readInts24(in, count, sortedDocIds);
//        break;
//      case BPV_32:
//        readInts32(in, count, sortedDocIds);
//        break;
//      default:
//        throw new IOException("Unsupported number of bits per value: " + bpv);
//    }
//
//    // Read positions
//    int bitsPerPosition = in.readByte() & 0xFF;
//    int numBytes = in.readInt();
//    byte[] packedPositions = new byte[numBytes];
//    in.readBytes(packedPositions, 0, numBytes);
//
//    // Unpack positions and reorder docIds
//    int[] positions = new int[count];
//    for (int i = 0; i < count; i++) {
//      positions[i] = readBits(packedPositions, i * bitsPerPosition, bitsPerPosition);
//    }
//
//    // Restore original order
//    for (int i = 0; i < count; i++) {
//      docIds[positions[i]] = sortedDocIds[i];
//    }
//  }
//
//  private void readSortedWithOrder1(IndexInput in, int count, int[] docIds) throws IOException {
//    //System.out.println("reading sorted");
//    // First read the sorted docIds using their original format
//    int[] sortedDocIds = new int[count];
//
//    // Read the format byte and handle accordingly
//    final int bpv = in.readByte();
//    switch (bpv) {
//      case CONTINUOUS_IDS:
//        readContinuousIds(in, count, sortedDocIds);
//        break;
//      case BITSET_IDS:
//        readBitSet(in, count, sortedDocIds);
//        break;
//      case DELTA_BPV_16:
//        readDelta16(in, count, sortedDocIds);
//        break;
//      case BPV_24:
//        readInts24(in, count, sortedDocIds);
//        break;
//      case BPV_32:
//        readInts32(in, count, sortedDocIds);
//        break;
//      default:
//        throw new IOException("Unsupported number of bits per value: " + bpv);
//    }
//
//    // Read position information
//    int bitsPerPosition = in.readByte() & 0xFF;
//    int numBlocks = in.readVInt();
//    final int BLOCK_SIZE = 32;
//
//    // Read and unpack positions block by block
//    for (int block = 0; block < numBlocks; block++) {
//      long packed = in.readLong();
//      int blockStart = block * BLOCK_SIZE;
//      int blockEnd = Math.min(blockStart + BLOCK_SIZE, count);
//
//      // Unpack positions for this block
//      for (int i = blockStart; i < blockEnd; i++) {
//        int position = (int)((packed >>> ((i - blockStart) * bitsPerPosition)) & ((1L << bitsPerPosition) - 1));
//        docIds[position] = sortedDocIds[i];
//      }
//    }
//  }
//
//  // Helper method to read bits
//  private static int readBits(byte[] array, int bitOffset, int numBits) {
//    int byteOffset = bitOffset / 8;
//    int bitInByte = bitOffset % 8;
//
//    int result = 0;
//    int bitsRemaining = numBits;
//
//    // Read first byte
//    int currentByte = array[byteOffset] & 0xFF;
//    result = currentByte >>> bitInByte;
//    bitsRemaining -= (8 - bitInByte);
//
//    // Read additional bytes as needed
//    int byteIndex = 1;
//    while (bitsRemaining > 0 && (byteOffset + byteIndex) < array.length) {
//      currentByte = array[byteOffset + byteIndex] & 0xFF;
//      result |= currentByte << (numBits - bitsRemaining);
//      bitsRemaining -= 8;
//      byteIndex++;
//    }
//
//    return result & ((1 << numBits) - 1); // Mask to required bits
//  }
//
//
//  private void readSortedWithOrder(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
//    // First read the sorted docIds using their original format
//    int[] sortedDocIds = new int[count];
//
//    // Read the format byte and handle accordingly
//    final int bpv = in.readByte();
//    switch (bpv) {
//      case CONTINUOUS_IDS:
//        readContinuousIds(in, count, sortedDocIds);
//        break;
//      case BITSET_IDS:
//        readBitSet(in, count, sortedDocIds);
//        break;
//      case DELTA_BPV_16:
//        readDelta16(in, count, sortedDocIds);
//        break;
//      case BPV_24:
//        readInts24(in, count, sortedDocIds);
//        break;
//      case BPV_32:
//        readInts32(in, count, sortedDocIds);
//        break;
//      default:
//        throw new IOException("Unsupported number of bits per value: " + bpv);
//    }
//
//    // Read positions
//    int bitsPerPosition = in.readByte() & 0xFF;
//    int numBytes = in.readInt();
//    byte[] packedPositions = new byte[numBytes];
//    in.readBytes(packedPositions, 0, numBytes);
//
//    // Use scratch array to store reordered docIds
//    for (int i = 0; i < count; i++) {
//      int position = readBits(packedPositions, i * bitsPerPosition, bitsPerPosition);
//      scratch[position] = sortedDocIds[i];
//    }
//
//    // Visit using scratchIntsRef
//    scratchIntsRef.ints = scratch;
//    scratchIntsRef.length = count;
//    visitor.visit(scratchIntsRef);
//  }
//
//  // Alternative version that visits one by one if memory is a concern
//  private void readSortedWithOrderOneByOne(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
//    // First read the sorted docIds
//    int[] sortedDocIds = new int[count];
//
//    // Read the format byte and handle accordingly
//    final int bpv = in.readByte();
//    switch (bpv) {
//      case CONTINUOUS_IDS:
//        readContinuousIds(in, count, sortedDocIds);
//        break;
//      case BITSET_IDS:
//        readBitSet(in, count, sortedDocIds);
//        break;
//      case DELTA_BPV_16:
//        readDelta16(in, count, sortedDocIds);
//        break;
//      case BPV_24:
//        readInts24(in, count, sortedDocIds);
//        break;
//      case BPV_32:
//        readInts32(in, count, sortedDocIds);
//        break;
//      default:
//        throw new IOException("Unsupported number of bits per value: " + bpv);
//    }
//
//    // Read the position information
//    int bitsNeeded = in.readVInt();
//    int positionsPerLong = 64 / bitsNeeded;
//    long mask = (1L << bitsNeeded) - 1;
//
//    // Create a mapping array: position -> index in sortedDocIds
//    int[] positionToIndex = new int[count];
//
//    // Read positions and build the mapping
//    int positionIndex = 0;
//    while (positionIndex < count) {
//      long packed = in.readLong();
//      int remainingPositions = Math.min(positionsPerLong, count - positionIndex);
//
//      for (int i = 0; i < remainingPositions; i++) {
//        int shift = 64 - bitsNeeded * (i + 1);
//        int position = (int)((packed >>> shift) & mask);
//        positionToIndex[position] = positionIndex++;
//      }
//    }
//
//    // Visit docIds in their original order
//    for (int i = 0; i < count; i++) {
//      visitor.visit(sortedDocIds[positionToIndex[i]]);
//    }
//  }
//
//  // Helper method to sort docIds while tracking original positions
//  private void sortWithPositions(int[] docIds, int[] positions, int left, int right) {
//    if (left < right) {
//      int mid = (left + right) >>> 1;
//      sortWithPositions(docIds, positions, left, mid);
//      sortWithPositions(docIds, positions, mid + 1, right);
//      mergeWithPositions(docIds, positions, left, mid, right);
//    }
//  }
//
//  private void mergeWithPositions(int[] docIds, int[] positions, int left, int mid, int right) {
//    int[] tempDocIds = new int[right - left + 1];
//    int[] tempPositions = new int[right - left + 1];
//
//    int i = left, j = mid + 1, k = 0;
//
//    while (i <= mid && j <= right) {
//      if (docIds[i] <= docIds[j]) {
//        tempDocIds[k] = docIds[i];
//        tempPositions[k] = positions[i];
//        i++;
//      } else {
//        tempDocIds[k] = docIds[j];
//        tempPositions[k] = positions[j];
//        j++;
//      }
//      k++;
//    }
//
//    while (i <= mid) {
//      tempDocIds[k] = docIds[i];
//      tempPositions[k] = positions[i];
//      i++;
//      k++;
//    }
//
//    while (j <= right) {
//      tempDocIds[k] = docIds[j];
//      tempPositions[k] = positions[j];
//      j++;
//      k++;
//    }
//
//    for (i = 0; i < k; i++) {
//      docIds[left + i] = tempDocIds[i];
//      positions[left + i] = tempPositions[i];
//    }
//  }
//
//  private static void writeIdsAsBitSet(int[] docIds, int start, int count, DataOutput out)
//      throws IOException {
//    int min = docIds[start];
//    int max = docIds[start + count - 1];
//
//    final int offsetWords = min >> 6;
//    final int offsetBits = offsetWords << 6;
//    final int totalWordCount = FixedBitSet.bits2words(max - offsetBits + 1);
//    long currentWord = 0;
//    int currentWordIndex = 0;
//
//    out.writeVInt(offsetWords);
//    out.writeVInt(totalWordCount);
//    // build bit set streaming
//    for (int i = 0; i < count; i++) {
//      final int index = docIds[start + i] - offsetBits;
//      final int nextWordIndex = index >> 6;
//      assert currentWordIndex <= nextWordIndex;
//      if (currentWordIndex < nextWordIndex) {
//        out.writeLong(currentWord);
//        currentWord = 0L;
//        currentWordIndex++;
//        while (currentWordIndex < nextWordIndex) {
//          currentWordIndex++;
//          out.writeLong(0L);
//        }
//      }
//      currentWord |= 1L << index;
//    }
//    out.writeLong(currentWord);
//    assert currentWordIndex + 1 == totalWordCount;
//  }
//
//  /** Read {@code count} integers into {@code docIDs}. */
//  void readInts(IndexInput in, int count, int[] docIDs) throws IOException {
//    final int bpv = in.readByte();
//    switch (bpv) {
//      case SORTED_WITH_ORDER:
//        readSortedWithOrder1(in, count, docIDs);
//        break;
//      case ROARING_BITMAP:
//        readRoaringBitmap(in, count, docIDs);
//        break;
//      case CONTINUOUS_IDS:
//        readContinuousIds(in, count, docIDs);
//        break;
//      case BITSET_IDS:
//        readBitSet(in, count, docIDs);
//        break;
//      case DELTA_BPV_16:
//        readDelta16(in, count, docIDs);
//        break;
//      case BPV_24:
//        readInts24(in, count, docIDs);
//        break;
//      case BPV_32:
//        readInts32(in, count, docIDs);
//        break;
//      case LEGACY_DELTA_VINT:
//        readLegacyDeltaVInts(in, count, docIDs);
//        break;
//      default:
//        throw new IOException("Unsupported number of bits per value: " + bpv);
//    }
//  }
//
//  private DocIdSetIterator readBitSetIterator(IndexInput in, int count) throws IOException {
//    int offsetWords = in.readVInt();
//    int longLen = in.readVInt();
//    scratchLongs.longs = ArrayUtil.growNoCopy(scratchLongs.longs, longLen);
//    in.readLongs(scratchLongs.longs, 0, longLen);
//    // make ghost bits clear for FixedBitSet.
//    if (longLen < scratchLongs.length) {
//      Arrays.fill(scratchLongs.longs, longLen, scratchLongs.longs.length, 0);
//    }
//    scratchLongs.length = longLen;
//    FixedBitSet bitSet = new FixedBitSet(scratchLongs.longs, longLen << 6);
//    return new DocBaseBitSetIterator(bitSet, count, offsetWords << 6);
//  }
//
//  private static void readContinuousIds(IndexInput in, int count, int[] docIDs) throws IOException {
//    int start = in.readVInt();
//    for (int i = 0; i < count; i++) {
//      docIDs[i] = start + i;
//    }
//  }
//
//  private static void readLegacyDeltaVInts(IndexInput in, int count, int[] docIDs)
//      throws IOException {
//    int doc = 0;
//    for (int i = 0; i < count; i++) {
//      doc += in.readVInt();
//      docIDs[i] = doc;
//    }
//  }
//
//  private void readBitSet(IndexInput in, int count, int[] docIDs) throws IOException {
//    DocIdSetIterator iterator = readBitSetIterator(in, count);
//    int docId, pos = 0;
//    while ((docId = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
//      docIDs[pos++] = docId;
//    }
//    assert pos == count : "pos: " + pos + ", count: " + count;
//  }
//
//  private static void readDelta16(IndexInput in, int count, int[] docIDs) throws IOException {
//    final int min = in.readVInt();
//    final int halfLen = count >>> 1;
//    in.readInts(docIDs, 0, halfLen);
//    for (int i = 0; i < halfLen; ++i) {
//      int l = docIDs[i];
//      docIDs[i] = (l >>> 16) + min;
//      docIDs[halfLen + i] = (l & 0xFFFF) + min;
//    }
//    if ((count & 1) == 1) {
//      docIDs[count - 1] = Short.toUnsignedInt(in.readShort()) + min;
//    }
//  }
//
//  private static void readInts24(IndexInput in, int count, int[] docIDs) throws IOException {
//    int i;
//    for (i = 0; i < count - 7; i += 8) {
//      long l1 = in.readLong();
//      long l2 = in.readLong();
//      long l3 = in.readLong();
//      docIDs[i] = (int) (l1 >>> 40);
//      docIDs[i + 1] = (int) (l1 >>> 16) & 0xffffff;
//      docIDs[i + 2] = (int) (((l1 & 0xffff) << 8) | (l2 >>> 56));
//      docIDs[i + 3] = (int) (l2 >>> 32) & 0xffffff;
//      docIDs[i + 4] = (int) (l2 >>> 8) & 0xffffff;
//      docIDs[i + 5] = (int) (((l2 & 0xff) << 16) | (l3 >>> 48));
//      docIDs[i + 6] = (int) (l3 >>> 24) & 0xffffff;
//      docIDs[i + 7] = (int) l3 & 0xffffff;
//    }
//    for (; i < count; ++i) {
//      docIDs[i] = (Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte());
//    }
//  }
//
//  private static void readInts32(IndexInput in, int count, int[] docIDs) throws IOException {
//    in.readInts(docIDs, 0, count);
//  }
//
//  private boolean shouldUseRoaringBitmap(int min, int max, int count) {
//    // Heuristic to decide when to use Roaring Bitmap
//    long range = (long)max - min;
//    double density = count / (double)range;
//
//    // Use Roaring Bitmap when:
//    // 1. Range is large enough
//    // 2. Density is neither too high nor too low
//    return range > 4096 && density > 0.01 && density < 0.7;
//  }
//
//  private void writeRoaringBitmap(int[] docIds, int start, int count, int min, int max, DataOutput out) throws IOException {
//    out.writeByte(ROARING_BITMAP);
//    out.writeVInt(count);
//
//
//    // Group by high bits (container index)
//    Map<Short, BitSet> containers = new HashMap<>();
//
//    for (int i = 0; i < count; i++) {
//      int docId = docIds[start + i];
//      short high = (short)(docId >>> 16);
//      int low = (docId & 0xFFFF);
//
//      containers.computeIfAbsent(high, k -> new BitSet()).set(low);
//    }
//
//    // Write number of containers
//    out.writeVInt(containers.size());
//
//    // Write containers in sorted order
//    TreeMap<Short, BitSet> sortedContainers = new TreeMap<>(containers);
//    for (Map.Entry<Short, BitSet> entry : sortedContainers.entrySet()) {
//      out.writeShort(entry.getKey());
//      BitSet bits = entry.getValue();
//
//      // Write the bitset efficiently
//      byte[] bytes = bits.toByteArray();
//      out.writeVInt(bytes.length);
//      out.writeBytes(bytes, 0, bytes.length);
//    }
//  }
//
//  private static class Container {
//    private final BitSet values = new BitSet(1 << 16);
//    private int cardinality = 0;
//
//    void add(int value) {
//      if (!values.get(value)) {
//        values.set(value);
//        cardinality++;
//      }
//    }
//
//    void writeTo(DataOutput out) throws IOException {
//      // Choose array or bitmap representation based on cardinality
//      if (cardinality < 4096) { // Array encoding
//        out.writeByte((byte) 0); // Array type
//        out.writeVInt(cardinality);
//        for (int i = values.nextSetBit(0); i >= 0; i = values.nextSetBit(i + 1)) {
//          out.writeShort((short)i);
//        }
//      } else { // Bitmap encoding
//        out.writeByte((byte) 1); // Bitmap type
//        byte[] bytes = values.toByteArray();
//        out.writeVInt(bytes.length);
//        out.writeBytes(bytes, 0, bytes.length);
//      }
//    }
//  }
//
//  private void readRoaringBitmap(IndexInput in, int count,  int[] docIds) throws IOException {
//    int expectedCount = in.readVInt();
//    assert expectedCount == count;
//
//    int containerCount = in.readVInt();
//    int docIdIndex = 0;
//
//    // Read containers in order (they were written in sorted order)
//    for (int i = 0; i < containerCount; i++) {
//      short high = in.readShort();
//      int byteCount = in.readVInt();
//      byte[] bytes = new byte[byteCount];
//      in.readBytes(bytes, 0, byteCount);
//
//      BitSet bits = BitSet.valueOf(bytes);
//
//      // Process bits in order to maintain sorting
//      for (int bit = bits.nextSetBit(0); bit >= 0; bit = bits.nextSetBit(bit + 1)) {
//        docIds[docIdIndex++] = (high << 16) | (bit & 0xFFFF);
//      }
//    }
//
//    assert docIdIndex == count : "Read " + docIdIndex + " docs, expected " + count;
//
//    // Verify sorting (can be disabled in production)
//    assert isSorted(docIds, count) : "DocIds are not properly sorted";
//  }
//
//  private static void readRoaringBitmap(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
//    int expectedCount = in.readVInt();
//    assert expectedCount == count;
//    int containerCount = in.readVInt();
//
//    for (int i = 0; i < containerCount; i++) {
//      short high = in.readShort();
//      int byteCount = in.readVInt();
//      byte[] bytes = new byte[byteCount];
//      in.readBytes(bytes, 0, byteCount);
//
//      BitSet bits = BitSet.valueOf(bytes);
//
//      // Process bits in order to maintain sorting
//      for (int bit = bits.nextSetBit(0); bit >= 0; bit = bits.nextSetBit(bit + 1)) {
//        int docId = (high << 16) | (bit & 0xFFFF);
//        visitor.visit(docId);
//      }
//    }
//  }
//
//  private boolean isSorted(int[] array, int count) {
//    for (int i = 1; i < count; i++) {
//      if (array[i - 1] >= array[i]) {
//        return false;
//      }
//    }
//    return true;
//  }
//
//
//
//  /**
//   * Read {@code count} integers and feed the result directly to {@link
//   * IntersectVisitor#visit(int)}.
//   */
//  void readInts(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
//    final int bpv = in.readByte();
//    switch (bpv) {
//      case SORTED_WITH_ORDER:
//        readSortedWithOrder(in, count, visitor);
//        break;
//      case ROARING_BITMAP:
//        readRoaringBitmap(in, count, visitor);
//        break;
//      case CONTINUOUS_IDS:
//        readContinuousIds(in, count, visitor);
//        break;
//      case BITSET_IDS:
//        readBitSet(in, count, visitor);
//        break;
//      case DELTA_BPV_16:
//        readDelta16(in, count, visitor);
//        break;
//      case BPV_24:
//        readInts24(in, count, visitor);
//        break;
//      case BPV_32:
//        readInts32(in, count, visitor);
//        break;
//      case LEGACY_DELTA_VINT:
//        readLegacyDeltaVInts(in, count, visitor);
//        break;
//      default:
//        throw new IOException("Unsupported number of bits per value: " + bpv);
//    }
//  }
//
//  private void readBitSet(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
//    DocIdSetIterator bitSetIterator = readBitSetIterator(in, count);
//    visitor.visit(bitSetIterator);
//  }
//
//  private static void readContinuousIds(IndexInput in, int count, IntersectVisitor visitor)
//      throws IOException {
//    int start = in.readVInt();
//    int extra = start & 63;
//    int offset = start - extra;
//    int numBits = count + extra;
//    FixedBitSet bitSet = new FixedBitSet(numBits);
//    bitSet.set(extra, numBits);
//    visitor.visit(new DocBaseBitSetIterator(bitSet, count, offset));
//  }
//
//  private static void readLegacyDeltaVInts(IndexInput in, int count, IntersectVisitor visitor)
//      throws IOException {
//    int doc = 0;
//    for (int i = 0; i < count; i++) {
//      doc += in.readVInt();
//      visitor.visit(doc);
//    }
//  }
//
//  private void readDelta16(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
//    readDelta16(in, count, scratch);
//    scratchIntsRef.ints = scratch;
//    scratchIntsRef.length = count;
//    visitor.visit(scratchIntsRef);
//  }
//
//  private static void readInts24(IndexInput in, int count, IntersectVisitor visitor)
//      throws IOException {
//    int i;
//    for (i = 0; i < count - 7; i += 8) {
//      long l1 = in.readLong();
//      long l2 = in.readLong();
//      long l3 = in.readLong();
//      visitor.visit((int) (l1 >>> 40));
//      visitor.visit((int) (l1 >>> 16) & 0xffffff);
//      visitor.visit((int) (((l1 & 0xffff) << 8) | (l2 >>> 56)));
//      visitor.visit((int) (l2 >>> 32) & 0xffffff);
//      visitor.visit((int) (l2 >>> 8) & 0xffffff);
//      visitor.visit((int) (((l2 & 0xff) << 16) | (l3 >>> 48)));
//      visitor.visit((int) (l3 >>> 24) & 0xffffff);
//      visitor.visit((int) l3 & 0xffffff);
//    }
//    for (; i < count; ++i) {
//      visitor.visit((Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte()));
//    }
//  }
//
//  private void readInts32(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
//    in.readInts(scratch, 0, count);
//    scratchIntsRef.ints = scratch;
//    scratchIntsRef.length = count;
//    visitor.visit(scratchIntsRef);
//  }
//}
