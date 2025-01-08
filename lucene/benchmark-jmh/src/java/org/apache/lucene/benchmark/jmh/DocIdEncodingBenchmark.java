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
package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Constants;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 8)
@Fork(value = 1)
public class DocIdEncodingBenchmark {

    private static final long BPV_21_MASK = 0x1FFFFFL;

    private static List<int[]> DOC_ID_SEQUENCES = new ArrayList<>();

    private static int INPUT_SCALE_FACTOR;

    static {
        parseInput();
    }

    @Param({
//            "Bit21With3StepsEncoder",
//            "Bit21With2StepsEncoder",
            //"Bit24Encoder",
//            "Bit21HybridEncoder",
//            "Bit21With2StepsOnlyRWLongEncoder",
//            "Bit21With3StepsEncoderOnlyRWLongEncoder",
            "RoaringBitmapEncoder1",
            "RoaringBitmapEncoder",
            "RoaringBitmapEncoderHighAndLowTogether",
            "RoaringBitmapEncoderOptimizedHighFirstLowSecond",
            "RoaringBitmapEncoderHighFirstLowSecond2",
            "RoaringBitmapEncoderLessOptimizedHighFirstLowSecond"
    })
    String encoderName;

    @Param({"encode", "decode"})
    String methodName;

    private DocIdEncoder docIdEncoder;

    private Path tmpDir;

    private final int[] scratch = new int[512];

    private String decoderInputFile;

    @Setup(Level.Trial)
    public void init() throws IOException {
        tmpDir = Files.createTempDirectory("docIdJmh");
        docIdEncoder = DocIdEncoder.SingletonFactory.fromName(encoderName);
        decoderInputFile =
                String.join("_", "docIdJmhData", docIdEncoder.getClass().getSimpleName(), "DecoderInput");
        // Create a file for decoders ( once per trial ) to read in every JMH iteration
        if (methodName.equalsIgnoreCase("decode")) {
            try (Directory dir = FSDirectory.open(tmpDir);
                 IndexOutput out = dir.createOutput(decoderInputFile, IOContext.DEFAULT)) {
                encode(out, docIdEncoder, DOC_ID_SEQUENCES, INPUT_SCALE_FACTOR);
            }
        }
    }

    @TearDown(Level.Trial)
    public void finish() throws IOException {
        if (methodName.equalsIgnoreCase("decode")) {
            Files.delete(tmpDir.resolve(decoderInputFile));
        }
        Files.delete(tmpDir);
    }

    @Benchmark
    public void executeEncodeOrDecode() throws IOException {
        if (methodName.equalsIgnoreCase("encode")) {
            String outputFile =
                    String.join(
                            "_",
                            "docIdJmhData",
                            docIdEncoder.getClass().getSimpleName(),
                            String.valueOf(System.nanoTime()));
            try (Directory dir = FSDirectory.open(tmpDir);
                 IndexOutput out = dir.createOutput(outputFile, IOContext.DEFAULT)) {
                encode(out, docIdEncoder, DOC_ID_SEQUENCES, INPUT_SCALE_FACTOR);
            } finally {
                Files.delete(tmpDir.resolve(outputFile));
            }
        } else if (methodName.equalsIgnoreCase("decode")) {
            try (Directory dir = FSDirectory.open(tmpDir);
                 IndexInput in = dir.openInput(decoderInputFile, IOContext.DEFAULT)) {
                for (int[] docIdSequence : DOC_ID_SEQUENCES) {
                    for (int i = 1; i <= INPUT_SCALE_FACTOR; i++) {
                        docIdEncoder.decode(in, 0, docIdSequence.length, scratch);
                    }
                }
            }
        } else {
            throw new IllegalArgumentException("Unknown method: " + methodName);
        }
    }

    private void encode(
            IndexOutput out, DocIdEncoder docIdEncoder, List<int[]> docIdSequences, int inputScaleFactor)
            throws IOException {
        for (int[] docIdSequence : docIdSequences) {
            for (int i = 1; i <= inputScaleFactor; i++) {
                docIdEncoder.encode(out, 0, docIdSequence.length, docIdSequence);
            }
        }
    }

    /**
     * Extend this interface to add a new implementation used for DocId Encoding and Decoding. These
     * are taken from org.apache.lucene.util.bkd.DocIdsWriter.
     */
    public interface DocIdEncoder {

        void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException;

        void decode(IndexInput in, int start, int count, int[] docIds) throws IOException;

        class SingletonFactory {

            private static final Map<String, DocIdEncoder> ENCODER_NAME_TO_INSTANCE_MAPPING =
                    new HashMap<>();

            static {
                initialiseEncoders();
            }

            private static String parsedClazzName(Class<?> clazz) {
                return clazz.getSimpleName().toLowerCase(Locale.ROOT);
            }

            private static void initialiseEncoders() {
                Class<?>[] allImplementations = DocIdEncoder.class.getDeclaredClasses();
                for (Class<?> clazz : allImplementations) {
                    boolean isADocIdEncoder =
                            Arrays.asList(clazz.getInterfaces()).contains(DocIdEncoder.class);
                    if (isADocIdEncoder) {
                        try {
                            ENCODER_NAME_TO_INSTANCE_MAPPING.put(
                                    parsedClazzName(clazz), (DocIdEncoder) clazz.getConstructor().newInstance());
                        } catch (InstantiationException
                                 | IllegalAccessException
                                 | InvocationTargetException
                                 | NoSuchMethodException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }

            public static DocIdEncoder fromName(String encoderName) {
                String parsedEncoderName = encoderName.trim().toLowerCase(Locale.ROOT);
                return getInternal(parsedEncoderName);
            }

            public static List<DocIdEncoder> getAllExcept(
                    List<Class<? extends DocIdEncoder>> excludeClasses) {
                return ENCODER_NAME_TO_INSTANCE_MAPPING.values().stream()
                        .filter(x -> !excludeClasses.contains(x.getClass()))
                        .toList();
            }

            private static DocIdEncoder getInternal(String parsedEncoderName) {
                if (ENCODER_NAME_TO_INSTANCE_MAPPING.containsKey(parsedEncoderName)) {
                    return ENCODER_NAME_TO_INSTANCE_MAPPING.get(parsedEncoderName);
                } else {
                    throw new IllegalArgumentException(
                            String.format(Locale.ROOT, "Unknown DocIdEncoder [%s]", parsedEncoderName));
                }
            }
        }

        class Bit24Encoder implements DocIdEncoder {
            @Override
            public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
                int i;
                for (i = 0; i < count - 7; i += 8) {
                    int doc1 = docIds[i];
                    int doc2 = docIds[i + 1];
                    int doc3 = docIds[i + 2];
                    int doc4 = docIds[i + 3];
                    int doc5 = docIds[i + 4];
                    int doc6 = docIds[i + 5];
                    int doc7 = docIds[i + 6];
                    int doc8 = docIds[i + 7];
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
                    out.writeShort((short) (docIds[i] >>> 8));
                    out.writeByte((byte) docIds[i]);
                }
            }

            @Override
            public void decode(IndexInput in, int start, int count, int[] docIDs) throws IOException {
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
                    docIDs[i] =
                            (Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte());
                }
            }
        }

        /**
         * Uses 21 bits to represent an integer and can store 3 docIds within a long. This is the
         * simplified version which is faster in encoding in aarch64
         */
        class Bit21With2StepsEncoder implements DocIdEncoder {
            @Override
            public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
                int i = 0;
                for (; i < count - 2; i += 3) {
                    long packedLong =
                            ((docIds[i] & BPV_21_MASK) << 42)
                                    | ((docIds[i + 1] & BPV_21_MASK) << 21)
                                    | (docIds[i + 2] & BPV_21_MASK);
                    out.writeLong(packedLong);
                }
                for (; i < count; i++) {
                    out.writeInt(docIds[i]);
                }
            }

            @Override
            public void decode(IndexInput in, int start, int count, int[] docIDs) throws IOException {
                int i = 0;
                for (; i < count - 2; i += 3) {
                    long packedLong = in.readLong();
                    docIDs[i] = (int) (packedLong >>> 42);
                    docIDs[i + 1] = (int) ((packedLong >>> 21) & BPV_21_MASK);
                    docIDs[i + 2] = (int) (packedLong & BPV_21_MASK);
                }
                for (; i < count; i++) {
                    docIDs[i] = in.readInt();
                }
            }
        }

        class Bit21With2StepsOnlyRWLongEncoder implements DocIdEncoder {
            @Override
            public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
                int i = 0;
                for (; i < count - 2; i += 3) {
                    long packedLong =
                            ((docIds[i] & BPV_21_MASK) << 42)
                                    | ((docIds[i + 1] & BPV_21_MASK) << 21)
                                    | (docIds[i + 2] & BPV_21_MASK);
                    out.writeLong(packedLong);
                }
                for (; i < count; i++) {
                    out.writeLong(docIds[i]);
                }
            }

            @Override
            public void decode(IndexInput in, int start, int count, int[] docIDs) throws IOException {
                int i = 0;
                for (; i < count - 2; i += 3) {
                    long packedLong = in.readLong();
                    docIDs[i] = (int) (packedLong >>> 42);
                    docIDs[i + 1] = (int) ((packedLong >>> 21) & BPV_21_MASK);
                    docIDs[i + 2] = (int) (packedLong & BPV_21_MASK);
                }
                for (; i < count; i++) {
                    docIDs[i] = (int) in.readLong();
                }
            }
        }

        /**
         * Variation of @{@link Bit21With2StepsEncoder} but uses 3 loops to decode the array of DocIds.
         * Comparatively better in decoding than @{@link Bit21With2StepsEncoder} on aarch64 with JDK 22
         * whereas poorer in encoding.
         */
        class Bit21With3StepsEncoder implements DocIdEncoder {

            @Override
            public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
                int i = 0;
                for (; i < count - 8; i += 9) {
                    long l1 =
                            ((docIds[i] & BPV_21_MASK) << 42)
                                    | ((docIds[i + 1] & BPV_21_MASK) << 21)
                                    | (docIds[i + 2] & BPV_21_MASK);
                    long l2 =
                            ((docIds[i + 3] & BPV_21_MASK) << 42)
                                    | ((docIds[i + 4] & BPV_21_MASK) << 21)
                                    | (docIds[i + 5] & BPV_21_MASK);
                    long l3 =
                            ((docIds[i + 6] & BPV_21_MASK) << 42)
                                    | ((docIds[i + 7] & BPV_21_MASK) << 21)
                                    | (docIds[i + 8] & BPV_21_MASK);
                    out.writeLong(l1);
                    out.writeLong(l2);
                    out.writeLong(l3);
                }
                for (; i < count - 2; i += 3) {
                    long packedLong =
                            ((docIds[i] & BPV_21_MASK) << 42)
                                    | ((docIds[i + 1] & BPV_21_MASK) << 21)
                                    | (docIds[i + 2] & BPV_21_MASK);
                    out.writeLong(packedLong);
                }
                for (; i < count; i++) {
                    out.writeInt(docIds[i]);
                }
            }

            @Override
            public void decode(IndexInput in, int start, int count, int[] docIDs) throws IOException {
                int i = 0;
                for (; i < count - 8; i += 9) {
                    long l1 = in.readLong();
                    long l2 = in.readLong();
                    long l3 = in.readLong();
                    docIDs[i] = (int) (l1 >>> 42);
                    docIDs[i + 1] = (int) ((l1 >>> 21) & BPV_21_MASK);
                    docIDs[i + 2] = (int) (l1 & BPV_21_MASK);
                    docIDs[i + 3] = (int) (l2 >>> 42);
                    docIDs[i + 4] = (int) ((l2 >>> 21) & BPV_21_MASK);
                    docIDs[i + 5] = (int) (l2 & BPV_21_MASK);
                    docIDs[i + 6] = (int) (l3 >>> 42);
                    docIDs[i + 7] = (int) ((l3 >>> 21) & BPV_21_MASK);
                    docIDs[i + 8] = (int) (l3 & BPV_21_MASK);
                }
                for (; i < count - 2; i += 3) {
                    long packedLong = in.readLong();
                    docIDs[i] = (int) (packedLong >>> 42);
                    docIDs[i + 1] = (int) ((packedLong >>> 21) & BPV_21_MASK);
                    docIDs[i + 2] = (int) (packedLong & BPV_21_MASK);
                }
                for (; i < count; i++) {
                    docIDs[i] = in.readInt();
                }
            }
        }

        class Bit21With3StepsEncoderOnlyRWLongEncoder implements DocIdEncoder {

            @Override
            public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
                int i = 0;
                for (; i < count - 8; i += 9) {
                    long l1 =
                            ((docIds[i] & BPV_21_MASK) << 42)
                                    | ((docIds[i + 1] & BPV_21_MASK) << 21)
                                    | (docIds[i + 2] & BPV_21_MASK);
                    long l2 =
                            ((docIds[i + 3] & BPV_21_MASK) << 42)
                                    | ((docIds[i + 4] & BPV_21_MASK) << 21)
                                    | (docIds[i + 5] & BPV_21_MASK);
                    long l3 =
                            ((docIds[i + 6] & BPV_21_MASK) << 42)
                                    | ((docIds[i + 7] & BPV_21_MASK) << 21)
                                    | (docIds[i + 8] & BPV_21_MASK);
                    out.writeLong(l1);
                    out.writeLong(l2);
                    out.writeLong(l3);
                }
                for (; i < count - 2; i += 3) {
                    long packedLong =
                            ((docIds[i] & BPV_21_MASK) << 42)
                                    | ((docIds[i + 1] & BPV_21_MASK) << 21)
                                    | (docIds[i + 2] & BPV_21_MASK);
                    out.writeLong(packedLong);
                }
                for (; i < count; i++) {
                    out.writeLong(docIds[i]);
                }
            }

            @Override
            public void decode(IndexInput in, int start, int count, int[] docIDs) throws IOException {
                int i = 0;
                for (; i < count - 8; i += 9) {
                    long l1 = in.readLong();
                    long l2 = in.readLong();
                    long l3 = in.readLong();
                    docIDs[i] = (int) (l1 >>> 42);
                    docIDs[i + 1] = (int) ((l1 >>> 21) & BPV_21_MASK);
                    docIDs[i + 2] = (int) (l1 & BPV_21_MASK);
                    docIDs[i + 3] = (int) (l2 >>> 42);
                    docIDs[i + 4] = (int) ((l2 >>> 21) & BPV_21_MASK);
                    docIDs[i + 5] = (int) (l2 & BPV_21_MASK);
                    docIDs[i + 6] = (int) (l3 >>> 42);
                    docIDs[i + 7] = (int) ((l3 >>> 21) & BPV_21_MASK);
                    docIDs[i + 8] = (int) (l3 & BPV_21_MASK);
                }
                for (; i < count - 2; i += 3) {
                    long packedLong = in.readLong();
                    docIDs[i] = (int) (packedLong >>> 42);
                    docIDs[i + 1] = (int) ((packedLong >>> 21) & BPV_21_MASK);
                    docIDs[i + 2] = (int) (packedLong & BPV_21_MASK);
                }
                for (; i < count; i++) {
                    docIDs[i] = (int) in.readLong();
                }
            }
        }

        class Bit21HybridEncoder implements DocIdEncoder {

            private final DocIdEncoder encoder;
            private final DocIdEncoder decoder;

            public Bit21HybridEncoder() {
                if (Constants.OS_ARCH.equals("aarch64")) {
                    this.encoder = this.decoder = new Bit21With2StepsEncoder();
                } else {
                    this.encoder = this.decoder = new Bit21With3StepsEncoderOnlyRWLongEncoder();
                }
            }

            @Override
            public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
                encoder.encode(out, start, count, docIds);
            }

            @Override
            public void decode(IndexInput in, int start, int count, int[] docIds) throws IOException {
                decoder.decode(in, start, count, docIds);
            }
        }

        /**
         * Last fallback in org.apache.lucene.util.bkd.DocIdsWriter#writeDocIds() when no optimisation
         * works
         */
        class Bit32Encoder implements DocIdEncoder {

            @Override
            public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
                for (int i = 0; i < count; i++) {
                    out.writeInt(docIds[i]);
                }
            }

            @Override
            public void decode(IndexInput in, int start, int count, int[] docIds) throws IOException {
                for (int i = 0; i < count; i++) {
                    docIds[i] = in.readInt();
                }
            }
        }

        /**
         * Variation of @{@link Bit32Encoder} using readLong and writeLong methods.
         */
        class Bit32OnlyRWLongEncoder implements DocIdEncoder {

            @Override
            public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
                int i;
                for (i = 0; i < count - 1; i += 2) {
                    long packedLong = (((long) docIds[i]) << 32) | docIds[i + 1];
                    out.writeLong(packedLong);
                }
                for (; i < count; i++) {
                    out.writeLong(docIds[i]);
                }
            }

            @Override
            public void decode(IndexInput in, int start, int count, int[] docIds) throws IOException {
                int i;
                for (i = 0; i < count - 1; i += 2) {
                    long packedLong = in.readLong();
                    docIds[i] = (int) (packedLong >>> 32);
                    docIds[i + 1] = (int) (packedLong & 0xFFFFFFFFL);
                }
                for (; i < count; i++) {
                    docIds[i] = (int) in.readLong();
                }
            }
        }

        class RoaringBitmapEncoder implements DocIdEncoder {
            @Override
            public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
                //out.writeInt(count);

                // First pass to count containers
                short numContainers = 1;   // Start with 1 since we'll have at least one container
                short lastHigh = (short) (docIds[start] >>> 16);  // Initialize with first value

                for (int i = 1; i < count; i++) {  // Start from second element
                    short high = (short) (docIds[start + i] >>> 16);
                    if (high != lastHigh) {
                        numContainers++;
                        lastHigh = high;
                    }
                }

                out.writeShort(numContainers);

                // Second pass to write data
                lastHigh = -1;
                int containerStart = 0;

                // Process in blocks of 8 when possible
                for (int i = 0; i < count; i++) {
                    int docId = docIds[start + i];
                    short high = (short) (docId >>> 16);

                    if (high != lastHigh) {
                        // Write previous container if exists
                        if (lastHigh != -1) {
                            writeContainer(out, lastHigh, docIds, start + containerStart, i - containerStart);
                        }
                        lastHigh = high;
                        containerStart = i;
                    }
                }

                // Write last container
                if (containerStart < count) {
                    writeContainer(out, lastHigh, docIds, start + containerStart, count - containerStart);
                }
            }

            private void writeContainer(IndexOutput out, short highBits, int[] docIds, int start, int count) throws IOException {
                out.writeShort(highBits);
                out.writeInt(count);

                // Write values in blocks of 8 when possible
                int i;
                for (i = 0; i + 7 < count; i += 8) {
                    long l1 = 0, l2 = 0;
                    for (int j = 0; j < 4; j++) {
                        l1 |= ((long) (docIds[start + i + j] & 0xFFFF) << (j * 16));
                        l2 |= ((long) (docIds[start + i + 4 + j] & 0xFFFF) << (j * 16));
                    }
                    out.writeLong(l1);
                    out.writeLong(l2);
                }

                // Write remaining values
                for (; i < count; i++) {
                    out.writeShort((short) docIds[start + i]);
                }
            }

            @Override
            public void decode(IndexInput in, int start, int count, int[] docIds) throws IOException {
//                int totalCount = in.readInt();
//                if (totalCount != count) {
//                    throw new IOException("Count mismatch");
//                }

                int containerCount = in.readShort();
                int pos = 0;

                for (int i = 0; i < containerCount; i++) {
                    short highBits = in.readShort();
                    int containerSize = in.readInt();

                    // Read blocks of 8
                    int j;
                    for (j = 0; j + 7 < containerSize; j += 8) {
                        long l1 = in.readLong();
                        long l2 = in.readLong();

                        for (int k = 0; k < 4; k++) {
                            docIds[pos++] = (highBits << 16) | (int) ((l1 >>> (k * 16)) & 0xFFFF);
                        }
                        for (int k = 0; k < 4; k++) {
                            docIds[pos++] = (highBits << 16) | (int) ((l2 >>> (k * 16)) & 0xFFFF);
                        }
                    }

                    // Read remaining values
                    for (; j < containerSize; j++) {
                        docIds[pos++] = (highBits << 16) | (in.readShort() & 0xFFFF);
                    }
                }

                if (pos != count) {
                    throw new IOException("Read " + pos + " values, expected " + count);
                }
            }
        }

//
        class RoaringBitmapEncoderLessOptimizedHighFirstLowSecond implements DocIdEncoder {
            @Override
            public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
               // out.writeVInt(count);

                // Count containers
                short numContainers = 1;
                short lastHigh = (short)(docIds[start] >>> 16);

                for (int i = 1; i < count; i++) {
                    short high = (short)(docIds[start + i] >>> 16);
                    if (high != lastHigh) {
                        numContainers++;
                        lastHigh = high;
                    }
                }
                out.writeShort(numContainers);

                // Write container info as longs (2 pairs of high bits and sizes per long)
                lastHigh = (short)(docIds[start] >>> 16);
                int containerStart = 0;
                long containerInfo = 0;
                int pairCount = 0;

                for (int i = 1; i < count; i++) {
                    short high = (short)(docIds[start + i] >>> 16);
                    if (high != lastHigh) {
                        // Pack container info into long
                        containerInfo |= ((long)lastHigh << (pairCount * 32)) | ((long)(i - containerStart) << (pairCount * 32 + 16));
                        pairCount++;

                        if (pairCount == 2) {
                            // Write packed container info
                            out.writeLong(containerInfo);
                            containerInfo = 0;
                            pairCount = 0;
                        }

                        lastHigh = high;
                        containerStart = i;
                    }
                }

                // Pack last container
                containerInfo |= ((long)lastHigh << (pairCount * 32)) | ((long)(count - containerStart) << (pairCount * 32 + 16));
                pairCount++;

                // Write final container info long if needed
                if (pairCount > 0) {
                    out.writeLong(containerInfo);
                }

                // Write low bits as longs (4 shorts per long)
                int i;
                for (i = 0; i + 3 < count; i += 4) {
                    long packed = 0L;
                    packed |= ((long)(docIds[start + i] & 0xFFFF));
                    packed |= ((long)(docIds[start + i + 1] & 0xFFFF) << 16);
                    packed |= ((long)(docIds[start + i + 2] & 0xFFFF) << 32);
                    packed |= ((long)(docIds[start + i + 3] & 0xFFFF) << 48);
                    out.writeLong(packed);
                }

                // Handle remaining values
                for (; i < count; i++) {
                    out.writeShort((short)(docIds[start + i] & 0xFFFF));
                }
            }

            @Override
            public void decode(IndexInput in, int start, int count, int[] docIds) throws IOException {

                short numContainers = in.readShort();
                short[] highBits = new short[numContainers];
                short[] containerSizes = new short[numContainers];

                for (int idx = 0; idx < numContainers - 1; idx += 2) {
                    long containerInfo = in.readLong();
                    highBits[idx] = (short)(containerInfo & 0xFFFF);
                    containerSizes[idx] = (short)((containerInfo >>> 16) & 0xFFFF);
                    highBits[idx + 1] = (short)((containerInfo >>> 32) & 0xFFFF);
                    containerSizes[idx + 1] = (short)((containerInfo >>> 48) & 0xFFFF);
                }

                if ((numContainers & 1) != 0) {
                    long containerInfo = in.readLong();
                    highBits[numContainers - 1] = (short)(containerInfo & 0xFFFF);
                    containerSizes[numContainers - 1] = (short)((containerInfo >>> 16) & 0xFFFF);
                }

                int pos = start;
                int containerIdx = 0;
                short currentHighBits = highBits[0];
                int remaining = containerSizes[0];

                while (pos < start + count) {
                    if (remaining >= 4 && pos + 3 < start + count) {
                        long packed = in.readLong();
                        docIds[pos] = (currentHighBits << 16) | (int)(packed & 0xFFFF);
                        docIds[pos + 1] = (currentHighBits << 16) | (int)((packed >>> 16) & 0xFFFF);
                        docIds[pos + 2] = (currentHighBits << 16) | (int)((packed >>> 32) & 0xFFFF);
                        docIds[pos + 3] = (currentHighBits << 16) | (int)((packed >>> 48) & 0xFFFF);
                        pos += 4;
                        remaining -= 4;
                    } else {
                        docIds[pos++] = (currentHighBits << 16) | (in.readShort() & 0xFFFF);
                        remaining--;
                    }

                    if (remaining == 0 && containerIdx + 1 < numContainers) {
                        currentHighBits = highBits[++containerIdx];
                        remaining = containerSizes[containerIdx];
                    }
                }
            }
        }

        class RoaringBitmapEncoderHighFirstLowSecond2 implements DocIdEncoder {
            @Override
            public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
                // Count containers and write container count
                short numContainers = 1;
                short lastHigh = (short)(docIds[start] >>> 16);

                for (int i = 1; i < count; i++) {
                    short high = (short)(docIds[start + i] >>> 16);
                    if (high != lastHigh) {
                        numContainers++;
                        lastHigh = high;
                    }
                }
                out.writeShort(numContainers);

                // First pass: Write all container headers
                lastHigh = (short)(docIds[start] >>> 16);
                int containerStart = 0;

                for (int i = 1; i < count; i++) {
                    short high = (short)(docIds[start + i] >>> 16);
                    if (high != lastHigh) {
                        int containerSize = i - containerStart;
                        out.writeInt((lastHigh << 16) | containerSize);
                        lastHigh = high;
                        containerStart = i;
                    }
                }
                // Write last container header
                out.writeInt((lastHigh << 16) | (count - containerStart));

//                // Second pass: Write all low bits in sequence
//                for (int i = 0; i < count; i++) {
//                    out.writeShort((short)(docIds[start + i] & 0xFFFF));
//                }

                // Second pass: Write all low bits in sequence, packing 4 shorts into a long
                int i = 0;
                for (; i < count - 3; i += 4) {
                    long packed = 0L;
                    packed |= ((long)(docIds[start + i] & 0xFFFF));
                    packed |= ((long)(docIds[start + i + 1] & 0xFFFF) << 16);
                    packed |= ((long)(docIds[start + i + 2] & 0xFFFF) << 32);
                    packed |= ((long)(docIds[start + i + 3] & 0xFFFF) << 48);
                    out.writeLong(packed);
                }
                // Handle remaining values
                for (; i < count; i++) {
                    out.writeShort((short)(docIds[start + i] & 0xFFFF));
                }
            }

            @Override
            public void decode(IndexInput in, int start, int count, int[] docIds) throws IOException {
                // Read container count
                short numContainers = in.readShort();

                // Read all container headers first
                int[] containerHighBits = new int[numContainers];
                int[] containerSizes = new int[numContainers];
                int totalValues = 0;

                for (int i = 0; i < numContainers; i++) {
                    int header = in.readInt();
                    containerHighBits[i] = header >>> 16;
                    containerSizes[i] = header & 0xFFFF;
                    totalValues += containerSizes[i];
                }

                // Now read all low bits and combine with appropriate high bits
                int pos = start;
                int containerIndex = 0;
                int remainingInContainer = containerSizes[0];
                int currentHigh = containerHighBits[0];

                // Read packed longs (4 shorts each)
                while (pos < start + count - 3) {
                    long packed = in.readLong();
                    for (int j = 0; j < 4 && pos < start + count; j++) {
                        if (remainingInContainer == 0) {
                            containerIndex++;
                            currentHigh = containerHighBits[containerIndex];
                            remainingInContainer = containerSizes[containerIndex];
                        }
                        int lowBits = (int)((packed >>> (16 * j)) & 0xFFFF);
                        docIds[pos++] = (currentHigh << 16) | lowBits;
                        remainingInContainer--;
                    }
                }

                // Handle remaining values
                while (pos < start + count) {
                    if (remainingInContainer == 0) {
                        containerIndex++;
                        currentHigh = containerHighBits[containerIndex];
                        remainingInContainer = containerSizes[containerIndex];
                    }
                    short lowBits = in.readShort();
                    docIds[pos++] = (currentHigh << 16) | (lowBits & 0xFFFF);
                    remainingInContainer--;
                }

                if (pos != start + count) {
                    throw new IOException("Decoded " + (pos - start) + " values, expected " + count);
                }
            }
        }

        // working one - final optimized
        class RoaringBitmapEncoderOptimizedHighFirstLowSecond implements DocIdEncoder {
            @Override
            public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
                // Count containers and write container count
                short numContainers = 1;
                short lastHigh = (short)(docIds[start] >>> 16);

                for (int i = 1; i < count; i++) {
                    short high = (short)(docIds[start + i] >>> 16);
                    if (high != lastHigh) {
                        numContainers++;
                        lastHigh = high;
                    }
                }
                out.writeShort(numContainers);

                // First pass: Write all container headers packed as longs (2 containers per long)
                lastHigh = (short)(docIds[start] >>> 16);
                int containerStart = 0;
                int containerIndex = 0;
                long[] packedHeaders = new long[numContainers / 2 + numContainers % 2];

                for (int i = 1; i < count; i++) {
                    short high = (short)(docIds[start + i] >>> 16);
                    if (high != lastHigh) {
                        short containerSize = (short)(i - containerStart);
                        // Pack container header into the appropriate position
                        if ((containerIndex & 1) == 0) {
                            packedHeaders[containerIndex >> 1] = ((long)lastHigh << 48) | ((long)containerSize << 32);
                        } else {
                            packedHeaders[containerIndex >> 1] |= ((long)lastHigh << 16) | containerSize;
                        }
                        containerIndex++;
                        lastHigh = high;
                        containerStart = i;
                    }
                }
                // Pack last container header
                short containerSize = (short)(count - containerStart);
                if ((containerIndex & 1) == 0) {
                    packedHeaders[containerIndex >> 1] = ((long)lastHigh << 48) | ((long)containerSize << 32);
                } else {
                    packedHeaders[containerIndex >> 1] |= ((long)lastHigh << 16) | containerSize;
                }

                // Write packed headers
                for (long packedHeader : packedHeaders) {
                    out.writeLong(packedHeader);
                }

                // Second pass: Write all low bits packed as longs (4 shorts per long)
                int i = 0;
                for (; i < count - 3; i += 4) {
                    long packed = 0L;
                    packed |= ((long)(docIds[start + i] & 0xFFFF));
                    packed |= ((long)(docIds[start + i + 1] & 0xFFFF) << 16);
                    packed |= ((long)(docIds[start + i + 2] & 0xFFFF) << 32);
                    packed |= ((long)(docIds[start + i + 3] & 0xFFFF) << 48);
                    out.writeLong(packed);
                }
                // Handle remaining values
                for (; i < count; i++) {
                    out.writeShort((short)(docIds[start + i] & 0xFFFF));
                }
            }

            @Override
            public void decode(IndexInput in, int start, int count, int[] docIds) throws IOException {
                // Read container count
                short numContainers = in.readShort();

                // Read all container headers packed as longs
                short[] containerHighBits = new short[numContainers];
                short[] containerSizes = new short[numContainers];

                for (int i = 0; i < numContainers; i += 2) {
                    long packedHeader = in.readLong();
                    containerHighBits[i] = (short)(packedHeader >>> 48);
                    containerSizes[i] = (short)((packedHeader >>> 32) & 0xFFFF);
                    if (i + 1 < numContainers) {
                        containerHighBits[i + 1] = (short)((packedHeader >>> 16) & 0xFFFF);
                        containerSizes[i + 1] = (short)(packedHeader & 0xFFFF);
                    }
                }

                // Now read all low bits and combine with appropriate high bits
                int pos = start;
                int containerIndex = 0;
                int remainingInContainer = containerSizes[0];
                int currentHigh = containerHighBits[0];

                // Read packed longs (4 shorts each)
                while (pos < start + count - 3) {
                    long packed = in.readLong();
                    for (int j = 0; j < 4 && pos < start + count; j++) {
                        if (remainingInContainer == 0) {
                            containerIndex++;
                            currentHigh = containerHighBits[containerIndex];
                            remainingInContainer = containerSizes[containerIndex];
                        }
                        int lowBits = (int)((packed >>> (16 * j)) & 0xFFFF);
                        docIds[pos++] = (currentHigh << 16) | lowBits;
                        remainingInContainer--;
                    }
                }

                // Handle remaining values
                while (pos < start + count) {
                    if (remainingInContainer == 0) {
                        containerIndex++;
                        currentHigh = containerHighBits[containerIndex];
                        remainingInContainer = containerSizes[containerIndex];
                    }
                    short lowBits = in.readShort();
                    docIds[pos++] = (currentHigh << 16) | (lowBits & 0xFFFF);
                    remainingInContainer--;
                }

                if (pos != start + count) {
                    throw new IOException("Decoded " + (pos - start) + " values, expected " + count);
                }
            }
        }

        // high and low together - works well
        class RoaringBitmapEncoder1 implements DocIdEncoder {
            @Override
            public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
                // Count containers
                short numContainers = 1;
                short lastHigh = (short)(docIds[start] >>> 16);
                for (int i = 1; i < count; i++) {
                    if ((short)(docIds[start + i] >>> 16) != lastHigh) {
                        numContainers++;
                        lastHigh = (short)(docIds[start + i] >>> 16);
                    }
                }
                out.writeShort(numContainers);

                // Write each container's header and packed low bits
                lastHigh = (short)(docIds[start] >>> 16);
                int containerStart = 0;

                for (int i = 1; i <= count; i++) {
                    if (i == count || (short)(docIds[start + i] >>> 16) != lastHigh) {
                        int containerSize = i - containerStart;

                        // Pack header into first 32 bits of a long, use remaining 32 bits for first two low values
                        long headerAndFirstValues = ((long)(lastHigh << 16 | containerSize) << 32);
                        if (containerSize > 0) {
                            headerAndFirstValues |= (docIds[start + containerStart] & 0xFFFF);
                        }
                        if (containerSize > 1) {
                            headerAndFirstValues |= ((long)(docIds[start + containerStart + 1] & 0xFFFF) << 16);
                        }
                        out.writeLong(headerAndFirstValues);

                        // Write remaining low bits, 4 per long
                        int j = containerStart + 2; // Start after the two values packed with header
                        for (; j <= containerStart + containerSize - 4; j += 4) {
                            out.writeLong(
                                    ((long)(docIds[start + j] & 0xFFFF)) |
                                            ((long)(docIds[start + j + 1] & 0xFFFF) << 16) |
                                            ((long)(docIds[start + j + 2] & 0xFFFF) << 32) |
                                            ((long)(docIds[start + j + 3] & 0xFFFF) << 48)
                            );
                        }

                        // Handle remaining values
                        if (j < containerStart + containerSize) {
                            long finalPacked = 0;
                            int shift = 0;
                            while (j < containerStart + containerSize) {
                                finalPacked |= ((long)(docIds[start + j] & 0xFFFF)) << shift;
                                j++;
                                shift += 16;
                            }
                            out.writeLong(finalPacked);
                        }

                        if (i < count) {
                            lastHigh = (short)(docIds[start + i] >>> 16);
                            containerStart = i;
                        }
                    }
                }
            }

            @Override
            public void decode(IndexInput in, int start, int count, int[] docIds) throws IOException {
                final short numContainers = in.readShort();
                int pos = start;

                // Process each container
                for (int i = 0; i < numContainers && pos < start + count; i++) {
                    // Read header and first two values
                    long headerAndFirstValues = in.readLong();
                    int header = (int)(headerAndFirstValues >>> 32);
                    int highBits = header >>> 16;
                    int containerSize = header & 0xFFFF;

                    // Extract first two values if they exist
                    if (containerSize > 0) {
                        docIds[pos++] = highBits << 16 | ((int)headerAndFirstValues & 0xFFFF);
                    }
                    if (containerSize > 1) {
                        docIds[pos++] = highBits << 16 | ((int)(headerAndFirstValues >>> 16) & 0xFFFF);
                    }

                    // Read remaining values
                    int j = 2;
                    for (; j <= containerSize - 4; j += 4) {
                        long packed = in.readLong();
                        docIds[pos++] = highBits << 16 | ((int)packed & 0xFFFF);
                        docIds[pos++] = highBits << 16 | ((int)(packed >>> 16) & 0xFFFF);
                        docIds[pos++] = highBits << 16 | ((int)(packed >>> 32) & 0xFFFF);
                        docIds[pos++] = highBits << 16 | ((int)(packed >>> 48) & 0xFFFF);
                    }

                    // Handle remaining values
                    if (j < containerSize) {
                        long packed = in.readLong();
                        int shift = 0;
                        while (j < containerSize) {
                            docIds[pos++] = highBits << 16 | ((int)(packed >>> shift) & 0xFFFF);
                            j++;
                            shift += 16;
                        }
                    }
                }
            }
        }
        // high and low packed together
        class RoaringBitmapEncoderHighAndLowTogether implements DocIdEncoder {
            @Override
            public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
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

            @Override
            public void decode(IndexInput in, int start, int count, int[] docIds) throws IOException {
                final short numContainers = in.readShort();
                int pos = start;

                // Process each container
                for (int i = 0; i < numContainers && pos < start + count; i++) {
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
        }
    }


    interface DocIdProvider {

        Map<Class<? extends DocIdEncodingBenchmark.DocIdEncoder>, Integer> ENCODER_TO_BPV_MAPPING = Map.ofEntries(
                Map.entry(DocIdEncodingBenchmark.DocIdEncoder.Bit21With2StepsEncoder.class, 21),
                Map.entry(DocIdEncodingBenchmark.DocIdEncoder.Bit21With3StepsEncoder.class, 21),
                Map.entry(DocIdEncodingBenchmark.DocIdEncoder.Bit21With2StepsOnlyRWLongEncoder.class, 21),
                Map.entry(DocIdEncodingBenchmark.DocIdEncoder.Bit21With3StepsEncoderOnlyRWLongEncoder.class, 21),
                Map.entry(DocIdEncodingBenchmark.DocIdEncoder.Bit21HybridEncoder.class, 21),
                Map.entry(DocIdEncodingBenchmark.DocIdEncoder.Bit24Encoder.class, 24),
                Map.entry(DocIdEncodingBenchmark.DocIdEncoder.Bit32Encoder.class, 32),
                Map.entry(DocIdEncoder.Bit32OnlyRWLongEncoder.class, 32),
                Map.entry(DocIdEncoder.RoaringBitmapEncoder.class, 32),
                Map.entry(DocIdEncoder.RoaringBitmapEncoder1.class, 32),
                Map.entry(DocIdEncoder.RoaringBitmapEncoderHighAndLowTogether.class, 32),
                Map.entry(DocIdEncoder.RoaringBitmapEncoderOptimizedHighFirstLowSecond.class, 32),
                Map.entry(DocIdEncoder.RoaringBitmapEncoderHighFirstLowSecond2.class, 32),
                Map.entry(DocIdEncoder.RoaringBitmapEncoderLessOptimizedHighFirstLowSecond.class, 32)
        );

        /**
         * We want to load all the docId sequences completely in memory to avoid including the time
         * spent in fetching from disk or any other source in every iteration unless we can consistently
         * prove otherwise. <br>
         *
         * @param args : Data about the source of docId sequences depending on the underlying provider
         *             like a file or randomly generated sequences given size.
         * @return : Loaded docIds
         */
        List<int[]> getDocIds(Object... args);
    }

    static class DocIdsFromLocalFS implements DocIdProvider {

        @Override
        public List<int[]> getDocIds(Object... args) {
            try (Stream<String> lines = Files.lines(Path.of((String) args[0]))) {
                return lines
                        .parallel()
                        .map(String::trim)
                        .filter(x -> !(x.startsWith("#") || x.isEmpty())) // Comments can start with a #
                        .map(
                                x ->
                                        Arrays.stream(x.split(","))
                                                .mapToInt((y -> Integer.parseInt(y.trim())))
                                                .toArray())
                        .toList();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class FixedBPVRandomDocIdProvider implements DocIdEncodingBenchmark.DocIdProvider {

        private static final Random RANDOM = new Random();

        private static final Map<Class<? extends DocIdEncoder>, Double> ENCODER_POWERS_OF_2;

        static {
            ENCODER_POWERS_OF_2 = new HashMap<>(ENCODER_TO_BPV_MAPPING.size());
            ENCODER_TO_BPV_MAPPING.forEach(
                    (encoderClazz, bitsUsed) ->
                            ENCODER_POWERS_OF_2.put(encoderClazz, Math.pow(2, bitsUsed) - 1));
        }

        @SuppressWarnings("unchecked")
        @Override
        public List<int[]> getDocIds(Object... args) {

            Class<? extends DocIdEncoder> encoderClass = (Class<? extends DocIdEncoder>) args[0];
            int capacity = (int) args[1];
            int low = (int) args[2];
            int high = (int) args[3];

            List<int[]> docIdSequences = new ArrayList<>(capacity);

            for (int i = 1; i <= capacity; i++) {
                docIdSequences.add(
                        RANDOM
                                .ints(0, ENCODER_POWERS_OF_2.get(encoderClass).intValue())
                                .distinct()
                                .limit(RANDOM.nextInt(low, high))
                                .sorted() // remove if needed
                                .toArray());
            }
            return docIdSequences;
        }
    }

    private static void parseInput() {

        String inputScaleFactor = System.getProperty("docIdEncoding.inputScaleFactor");

        if (inputScaleFactor != null && !inputScaleFactor.isEmpty()) {
            INPUT_SCALE_FACTOR = Integer.parseInt(inputScaleFactor);
        } else {
            INPUT_SCALE_FACTOR = 10;
        }

        String inputFilePath = System.getProperty("docIdEncoding.inputFile");
        if (inputFilePath != null && !inputFilePath.isEmpty()) {
            DOC_ID_SEQUENCES = new DocIdsFromLocalFS().getDocIds(inputFilePath);
        } else {
            DOC_ID_SEQUENCES =
                    new FixedBPVRandomDocIdProvider()
                            .getDocIds(DocIdEncoder.Bit21With3StepsEncoder.class, 100, 100, 512);
        }
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(DocIdEncodingBenchmark.class.getSimpleName())
                .forks(1)
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(5))
                .measurementIterations(5)
                .measurementTime(TimeValue.seconds(8))
                .threads(1)
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .build();

        new Runner(opt).run();
    }
}