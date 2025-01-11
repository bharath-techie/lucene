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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;

import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.DocBaseBitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;

final class DocIdsWriter {

  private static final byte CONTINUOUS_IDS = (byte) -2;
  private static final byte BITSET_IDS = (byte) -1;
  private static final byte DELTA_BPV_16 = (byte) 16;
  private static final byte BPV_24 = (byte) 24;
  private static final byte BPV_32 = (byte) 32;
  private static final byte ROARING = (byte) 1;
  // These signs are legacy, should no longer be used in the writing side.
  private static final byte LEGACY_DELTA_VINT = (byte) 0;
  private static final byte ROARING_BITMAP = (byte) 6; // New format identifier

  private final int[] scratch;

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

  // Declare these as instance variables to reuse across calls
  private short[] containerHighBits;
  private short[] containerSizes;
  private static final Path TEMP_DIR_FOR_DOCIDS;
  static {
    try {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss_", Locale.ROOT);
      String formattedNow = ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(formatter);
      TEMP_DIR_FOR_DOCIDS = Files.createTempDirectory("docIds_" + formattedNow);
      System.out.println("Temp directory where docIds will be written is " + TEMP_DIR_FOR_DOCIDS);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  private boolean createdForWriting = false;
  private PrintWriter rawDocIdFileWriter;
  private Path docIdsFile;
  private long numDocIdSequencesWritten = 0L;



  // Initialize or resize arrays when needed
  private void ensureArrayCapacity(int numContainers) {
    if (containerHighBits == null || containerHighBits.length < numContainers) {
      containerHighBits = new short[numContainers];
      containerSizes = new short[numContainers];
    }
  }

  {
    // This is here to not rely on the default constructor of IntsRef to set offset to 0
    scratchIntsRef.offset = 0;
  }

  DocIdsWriter(int maxPointsInLeaf, Object... args) {
    scratch = new int[maxPointsInLeaf];
    if (args != null && args.length > 0) {
      this.createdForWriting = (boolean) args[0];
      try {
        docIdsFile = TEMP_DIR_FOR_DOCIDS.resolve(UUID.randomUUID() + "_" + System.nanoTime());
        this.rawDocIdFileWriter =
                new PrintWriter(
                        new BufferedWriter(
                                new FileWriter(docIdsFile.toString(), Charset.defaultCharset())));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  void writeDocIds(int[] docIds, int start, int count, DataOutput out) throws IOException {
    // docs can be sorted either when all docs in a block have the same value
    // or when a segment is sorted
    boolean strictlySorted = true;
    int min = docIds[0];
    int max = docIds[0];
    short numContainers = 1;
    short lastHigh = (short)(docIds[start] >>> 16);
    for (int i = 1; i < count; ++i) {
      int last = docIds[start + i - 1];
      int current = docIds[start + i];
      if (last >= current) {
        strictlySorted = false;
        numContainers++;
        lastHigh = (short)(docIds[start + i] >>> 16);
      } else {
        if ((short)(docIds[start + i] >>> 16) != lastHigh) {
          numContainers++;
          lastHigh = (short)(docIds[start + i] >>> 16);
        }
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
      } else if (min2max <= 0xFFFF) {
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
        return;
      } else {
        if(createdForWriting) {
          numDocIdSequencesWritten++;
          rawDocIdFileWriter.println(Arrays.toString(docIds).replace("[", "").replace("]", ""));
        }
        out.writeByte(ROARING);
        writeRoaringBitmap(docIds, start, count, numContainers, out);
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
    } else if(shouldUseRoaringBitmap(min2max, max, numContainers)) {
      numDocIdSequencesWritten++;
      rawDocIdFileWriter.println(Arrays.toString(docIds).replace("[", "").replace("]", ""));

      out.writeByte(ROARING_BITMAP);
      writeRoaringBitmap(docIds, start, count, numContainers, out);
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

  private boolean shouldUseRoaringBitmap(int min2max, int max, int noOfContainers) {
    if (min2max <= 0xFFFF) {
      return false;
    }
    if(max <= 0xFFFFFF) {
      return noOfContainers <= 100;
    } else {
      return noOfContainers <= 200;
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
    assert currentWordIndex + 1 == totalWordCount;
  }

  private void writeRoaringBitmap1(int[] docIds, int start, int count, DataOutput out) throws IOException {
    // Count and write containers
    short numContainers = 1;
    short lastHigh = (short)(docIds[start] >>> 16);
    for (int i = 1; i < count; i++) {
      if ((short)(docIds[start + i] >>> 16) != lastHigh) {
        numContainers++;
        lastHigh = (short)(docIds[start + i] >>> 16);
      }
    }
    out.writeShort(numContainers);

    // Write each container's header followed by its low bits
    lastHigh = (short)(docIds[start] >>> 16);
    int containerStart = 0;

    for (int i = 1; i <= count; i++) {
      if (i == count || (short)(docIds[start + i] >>> 16) != lastHigh) {
        int containerSize = i - containerStart;
        out.writeInt((lastHigh << 16) | containerSize);

        // Write this container's low bits packed
        int j = containerStart;
        for (; j <= containerStart + containerSize - 4; j += 4) {
          out.writeLong(((long)(docIds[start + j] & 0xFFFF)) |
                  ((long)(docIds[start + j + 1] & 0xFFFF) << 16) |
                  ((long)(docIds[start + j + 2] & 0xFFFF) << 32) |
                  ((long)(docIds[start + j + 3] & 0xFFFF) << 48));
        }
        for (; j < containerStart + containerSize; j++) {
          out.writeShort((short)(docIds[start + j] & 0xFFFF));
        }

        if (i < count) {
          lastHigh = (short)(docIds[start + i] >>> 16);
          containerStart = i;
        }
      }
    }
  }

  private void writeRoaringBitmap(int[] docIds, int start, int count, short numContainers, DataOutput out) throws IOException {
    out.writeShort(numContainers);

    // First pass: determine container boundaries
    short[] containerStarts = new short[numContainers];
    short[] containerSizes = new short[numContainers];

    int containerIndex = 0;
    containerStarts[0] = (short) start;
    short lastHigh = (short)(docIds[start] >>> 16);

    for (int i = 1; i < count; i++) {
      int current = docIds[start + i];
      int previous = docIds[start + i - 1];
      short currentHigh = (short)(current >>> 16);

      if (previous >= current || currentHigh != lastHigh) {
        containerSizes[containerIndex] = (short)(i - containerStarts[containerIndex]);
        containerIndex++;
        if (containerIndex < numContainers) {
          containerStarts[containerIndex] = (short) (start + i);
          lastHigh = currentHigh;
        }
      }
    }
    // Set size for the last container
    containerSizes[containerIndex] = (short)(count - (containerStarts[containerIndex] - start));

    // Write container headers (2 per long)
    for (int i = 0; i < numContainers; i += 2) {
      long packedHeader = 0;
      packedHeader = ((long)(docIds[containerStarts[i]] >>> 16) << 48) | ((long)containerSizes[i] << 32);
      if (i + 1 < numContainers) {
        packedHeader |= ((long)(docIds[containerStarts[i + 1]] >>> 16) << 16) | containerSizes[i + 1];
      }
      out.writeLong(packedHeader);
    }

    // Write the low bits for each container
    for (int i = 0; i < numContainers; i++) {
      int containerStart = containerStarts[i];
      int containerSize = containerSizes[i];
      int pos = 0;

      // Write full longs (4 shorts)
      while (pos < containerSize - 3) {
        long packed = 0L;
        packed |= ((long)(docIds[containerStart + pos] & 0xFFFF));
        packed |= ((long)(docIds[containerStart + pos + 1] & 0xFFFF) << 16);
        packed |= ((long)(docIds[containerStart + pos + 2] & 0xFFFF) << 32);
        packed |= ((long)(docIds[containerStart + pos + 3] & 0xFFFF) << 48);
        out.writeLong(packed);
        pos += 4;
      }

      // Write remaining values in container
      while (pos < containerSize) {
        out.writeShort((short)(docIds[containerStart + pos] & 0xFFFF));
        pos++;
      }
    }
  }

  private void readRoaringBitmap1(IndexInput in, int count, int[] docIds) throws IOException {
    final short numContainers = in.readShort();
    int pos = 0;

    // Process each container
    for (int i = 0; i < numContainers && pos <  count; i++) {
      int header = in.readInt();
      int highBits = header >>> 16;
      int containerSize = header & 0xFFFF;

      // Read this container's low bits
      int j = 0;
      for (; j <= containerSize - 4; j += 4) {
        long packed = in.readLong();
        docIds[pos++] = highBits << 16 | ((int)packed & 0xFFFF);
        docIds[pos++] = highBits << 16 | ((int)(packed >>> 16) & 0xFFFF);
        docIds[pos++] = highBits << 16 | ((int)(packed >>> 32) & 0xFFFF);
        docIds[pos++] = highBits << 16 | ((int)(packed >>> 48) & 0xFFFF);
      }
      for (; j < containerSize; j++) {
        docIds[pos++] = highBits << 16 | (in.readShort() & 0xFFFF);
      }
    }

  }

  private void readRoaringBitmap(IndexInput in, int count, int[] docIds) throws IOException {
    short numContainers = in.readShort();
    ensureArrayCapacity(numContainers);

    // Read container headers
    for (int i = 0; i < numContainers; i += 2) {
      long packedHeader = in.readLong();
      containerHighBits[i] = (short)(packedHeader >>> 48);
      containerSizes[i] = (short)((packedHeader >>> 32) & 0xFFFF);
      if (i + 1 < numContainers) {
        containerHighBits[i + 1] = (short)((packedHeader >>> 16) & 0xFFFF);
        containerSizes[i + 1] = (short)(packedHeader & 0xFFFF);
      }
    }

    int pos = 0;
    // Read each container's data
    for (int containerIndex = 0; containerIndex < numContainers; containerIndex++) {
      int containerSize = containerSizes[containerIndex];
      int currentHigh = containerHighBits[containerIndex];
      int containerPos = 0;

      // Read full longs (4 shorts)
      while (containerPos < containerSize - 3) {
        int highbits = currentHigh << 16;
        long packed = in.readLong();

        docIds[pos++] = (highbits ) | ((int)packed & 0xFFFF);
        docIds[pos++] = (highbits) | ((int)(packed >>> 16) & 0xFFFF);
        docIds[pos++] = (highbits) | ((int)(packed >>> 32) & 0xFFFF);
        docIds[pos++] = (highbits) | ((int)(packed >>> 48) & 0xFFFF);

        containerPos += 4;
      }

      // Read remaining values in container
      while (containerPos < containerSize) {
        short lowBits = in.readShort();
        docIds[pos++] = (currentHigh << 16) | (lowBits & 0xFFFF);
        containerPos++;
      }
    }

    if (pos != count) {
      throw new IOException("Decoded " + pos + " values, expected " + count);
    }
  }

  private void readRoaringBitmap(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    readRoaringBitmap(in, count, scratch);
    scratchIntsRef.ints = scratch;
    scratchIntsRef.length = count;
    visitor.visit(scratchIntsRef);
  }

  /** Read {@code count} integers into {@code docIDs}. */
  void readInts(IndexInput in, int count, int[] docIDs) throws IOException {
    final int bpv = in.readByte();
    switch (bpv) {
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
      case ROARING:
        readRoaringBitmap(in, count, docIDs);
        break;
      case LEGACY_DELTA_VINT:
        readLegacyDeltaVInts(in, count, docIDs);
        break;
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
    }
  }

  private static DocIdSetIterator readBitSetIterator(IndexInput in, int count) throws IOException {
    int offsetWords = in.readVInt();
    int longLen = in.readVInt();
    long[] bits = new long[longLen];
    in.readLongs(bits, 0, longLen);
    FixedBitSet bitSet = new FixedBitSet(bits, longLen << 6);
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

  private static void readBitSet(IndexInput in, int count, int[] docIDs) throws IOException {
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
      case ROARING:
        readRoaringBitmap(in, count, visitor);
        break;
      case LEGACY_DELTA_VINT:
        readLegacyDeltaVInts(in, count, visitor);
        break;
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
    }
  }

  private static void readBitSet(IndexInput in, int count, IntersectVisitor visitor)
          throws IOException {
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

  public void close() {
    if (createdForWriting && rawDocIdFileWriter != null) {
      rawDocIdFileWriter.close();
      if (numDocIdSequencesWritten == 0) {
        try {
          // To avoid creating a zero byte file.
          Files.delete(docIdsFile);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        System.out.println("Written " + numDocIdSequencesWritten + " docId sequences");
      }
    }
  }
}
