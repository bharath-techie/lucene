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

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 8)
@Fork(value = 1)
public class DocIdEncodingBenchmark1 {

    private static final long BPV_21_MASK = 0x1FFFFFL;

    private static List<int[]> DOC_ID_SEQUENCES = new ArrayList<>();

    private static int INPUT_SCALE_FACTOR;

    static {
        parseInput();
    }

    @Param({
    //        "Bit21With3StepsEncoder",
//            "Bit21With2StepsEncoder",
   //         "Bit24Encoder",
//            "Bit21HybridEncoder",
//            "Bit21With2StepsOnlyRWLongEncoder",
//            "Bit21With3StepsEncoderOnlyRWLongEncoder",
            //"RoaringBitmapEncoder1",
            "RoaringBitmapEncoderPacked",
           // "DeltaDocIdEncoder"
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


        // high and low together - works well
        class RoaringBitmapEncoder1 implements DocIdEncoder {
            @Override
            public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
                // Count containers
                short numContainers = 1;
                short lastHigh = (short) (docIds[start] >>> 16);
                int i = start + 1;
                int end = start + count;

                while (i < end) {
                    if ((short) (docIds[i] >>> 16) != lastHigh) {
                        numContainers++;
                        lastHigh = (short) (docIds[i] >>> 16);
                    }
                    i++;
                }
                out.writeShort(numContainers);

                // Write containers
                lastHigh = (short) (docIds[start] >>> 16);
                int containerStart = start;
                i = start + 1;

                while (i <= end) {
                    if (i == end || (short) (docIds[i] >>> 16) != lastHigh) {
                        int containerSize = i - containerStart;

                        // Write header as int (high 16 bits for lastHigh, low 16 bits for size)
                        out.writeInt((lastHigh << 16) | containerSize);

                        // Write docIds 4 at a time in longs
                        int j = containerStart;
                        int endMinus4 = i - 4;

                        while (j <= endMinus4) {
                            out.writeLong(
                                    ((long) (docIds[j] & 0xFFFF)) |
                                            ((long) (docIds[j + 1] & 0xFFFF) << 16) |
                                            ((long) (docIds[j + 2] & 0xFFFF) << 32) |
                                            ((long) (docIds[j + 3] & 0xFFFF) << 48)
                            );
                            j += 4;
                        }

                        // Handle remaining values (0-3)
                        if (j < i) {
                            long finalPacked = 0;
                            int remaining = i - j;
                            if (remaining >= 1) finalPacked |= (long) (docIds[j] & 0xFFFF);
                            if (remaining >= 2) finalPacked |= (long) (docIds[j + 1] & 0xFFFF) << 16;
                            if (remaining >= 3) finalPacked |= (long) (docIds[j + 2] & 0xFFFF) << 32;
                            out.writeLong(finalPacked);
                        }

                        if (i < end) {
                            lastHigh = (short) (docIds[i] >>> 16);
                            containerStart = i;
                        }
                    }
                    i++;
                }
            }

            @Override
            public void decode(IndexInput in, int start, int count, int[] docIds) throws IOException {
                final short numContainers = in.readShort();
                int pos = start;
                int end = start + count;

                for (short i = 0; i < numContainers && pos < end; i++) {
                    // Read container header
                    int header = in.readInt();
                    short highBits = (short) (header >>> 16);
                    int containerSize = header & 0xFFFF;

                    // Process values in chunks of 12 (3 longs)
                    int remaining = containerSize;
                    while (remaining >= 12) {
                        long packed1 = in.readLong();
                        long packed2 = in.readLong();
                        long packed3 = in.readLong();

                        int highBitsShifted = highBits << 16;

                        // Unpack 12 values
                        docIds[pos] = highBitsShifted | (int) (packed1 & 0xFFFF);
                        docIds[pos + 1] = highBitsShifted | (int) (packed1 >>> 16 & 0xFFFF);
                        docIds[pos + 2] = highBitsShifted | (int) (packed1 >>> 32 & 0xFFFF);
                        docIds[pos + 3] = highBitsShifted | (int) (packed1 >>> 48 & 0xFFFF);

                        docIds[pos + 4] = highBitsShifted | (int) (packed2 & 0xFFFF);
                        docIds[pos + 5] = highBitsShifted | (int) (packed2 >>> 16 & 0xFFFF);
                        docIds[pos + 6] = highBitsShifted | (int) (packed2 >>> 32 & 0xFFFF);
                        docIds[pos + 7] = highBitsShifted | (int) (packed2 >>> 48 & 0xFFFF);

                        docIds[pos + 8] = highBitsShifted | (int) (packed3 & 0xFFFF);
                        docIds[pos + 9] = highBitsShifted | (int) (packed3 >>> 16 & 0xFFFF);
                        docIds[pos + 10] = highBitsShifted | (int) (packed3 >>> 32 & 0xFFFF);
                        docIds[pos + 11] = highBitsShifted | (int) (packed3 >>> 48 & 0xFFFF);

                        pos += 12;
                        remaining -= 12;
                    }

                    // Handle remaining values (less than 12)
                    while (remaining >= 4) {
                        long packed = in.readLong();
                        docIds[pos++] = highBits << 16 | (int) (packed & 0xFFFF);
                        docIds[pos++] = highBits << 16 | (int) (packed >>> 16 & 0xFFFF);
                        docIds[pos++] = highBits << 16 | (int) (packed >>> 32 & 0xFFFF);
                        docIds[pos++] = highBits << 16 | (int) (packed >>> 48 & 0xFFFF);
                        remaining -= 4;
                    }

                    if (remaining > 0) {
                        long packed = in.readLong();
                        if (remaining >= 1) docIds[pos++] = highBits << 16 | (int) (packed & 0xFFFF);
                        if (remaining >= 2) docIds[pos++] = highBits << 16 | (int) (packed >>> 16 & 0xFFFF);
                        if (remaining >= 3) docIds[pos++] = highBits << 16 | (int) (packed >>> 32 & 0xFFFF);
                    }
                }
            }
        }

        class RoaringBitmapEncoderPacked implements DocIdEncoder {
            @Override
            public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
                short numContainers = 1;
                short lastHigh = (short)(docIds[start] >>> 16);
                for (int i = 1; i < count; i++) {
                    if ((short)(docIds[start + i] >>> 16) != lastHigh) {
                        numContainers++;
                        lastHigh = (short)(docIds[start + i] >>> 16);
                    }
                }
                out.writeShort(numContainers);

                lastHigh = (short)(docIds[start] >>> 16);
                int containerStart = 0;
                for (int i = 1; i <= count; i++) {
                    if (i == count || (short)(docIds[start + i] >>> 16) != lastHigh) {
                        int containerSize = i - containerStart;
                        out.writeInt((lastHigh << 16) | containerSize);

                        int j = containerStart;
                        while (j + 8 <= containerStart + containerSize) {
                            out.writeLong(((long)(docIds[start + j] & 0xFFFF)) |
                                    ((long)(docIds[start + j + 1] & 0xFFFF) << 16) |
                                    ((long)(docIds[start + j + 2] & 0xFFFF) << 32) |
                                    ((long)(docIds[start + j + 3] & 0xFFFF) << 48));
                            out.writeLong(((long)(docIds[start + j + 4] & 0xFFFF)) |
                                    ((long)(docIds[start + j + 5] & 0xFFFF) << 16) |
                                    ((long)(docIds[start + j + 6] & 0xFFFF) << 32) |
                                    ((long)(docIds[start + j + 7] & 0xFFFF) << 48));
                            j += 8;
                        }
                        if (j + 4 <= containerStart + containerSize) {
                            out.writeLong(((long)(docIds[start + j] & 0xFFFF)) |
                                    ((long)(docIds[start + j + 1] & 0xFFFF) << 16) |
                                    ((long)(docIds[start + j + 2] & 0xFFFF) << 32) |
                                    ((long)(docIds[start + j + 3] & 0xFFFF) << 48));
                            j += 4;
                        }
                        if (j + 2 <= containerStart + containerSize) {
                            out.writeInt(((int)(docIds[start + j] & 0xFFFF)) |
                                    ((int)(docIds[start + j + 1] & 0xFFFF) << 16));
                            j += 2;
                        }
                        if (j < containerStart + containerSize) {
                            out.writeShort((short) (docIds[start + j] & 0xFFFF));
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

                for (int i = 0; i < numContainers; i++) {
                    int header = in.readInt();
                    int highBits = header >>> 16;
                    int containerSize = header & 0xFFFF;
                    int highShifted = highBits << 16;
                    int j = 0;
                    while (j + 8 <= containerSize) {
                        long packed1 = in.readLong();
                        long packed2 = in.readLong();
                        docIds[pos++] = highShifted| ((int)packed1 & 0xFFFF);
                        docIds[pos++] = highShifted | ((int)(packed1 >>> 16) & 0xFFFF);
                        docIds[pos++] = highShifted | ((int)(packed1 >>> 32) & 0xFFFF);
                        docIds[pos++] = highShifted | ((int)(packed1 >>> 48) & 0xFFFF);
                        docIds[pos++] = highShifted | ((int)packed2 & 0xFFFF);
                        docIds[pos++] = highShifted | ((int)(packed2 >>> 16) & 0xFFFF);
                        docIds[pos++] = highShifted | ((int)(packed2 >>> 32) & 0xFFFF);
                        docIds[pos++] = highShifted | ((int)(packed2 >>> 48) & 0xFFFF);
                        j += 8;
                    }
                    if (j + 4 <= containerSize) {
                        long packed = in.readLong();
                        docIds[pos++] = highShifted | ((int)packed & 0xFFFF);
                        docIds[pos++] = highShifted | ((int)(packed >>> 16) & 0xFFFF);
                        docIds[pos++] = highShifted | ((int)(packed >>> 32) & 0xFFFF);
                        docIds[pos++] = highShifted | ((int)(packed >>> 48) & 0xFFFF);
                        j += 4;
                    }
                    if(j + 2 <= containerSize) {
                        int packed = in.readInt();
                        docIds[pos++] = highShifted | ((int)packed & 0xFFFF);
                        docIds[pos++] = highShifted | ((int)(packed >>> 16) & 0xFFFF);
                        j+=2;
                    }
                    if (j < containerSize) {
                        docIds[pos++] = highShifted | in.readShort();
                    }

                }
            }
        }
    }


    interface DocIdProvider {

        Map<Class<? extends DocIdEncodingBenchmark1.DocIdEncoder>, Integer> ENCODER_TO_BPV_MAPPING = Map.ofEntries(
                Map.entry(DocIdEncodingBenchmark1.DocIdEncoder.Bit21With2StepsEncoder.class, 21),
                Map.entry(DocIdEncodingBenchmark1.DocIdEncoder.Bit21With3StepsEncoder.class, 21),
                Map.entry(DocIdEncodingBenchmark1.DocIdEncoder.Bit21With2StepsOnlyRWLongEncoder.class, 21),
                Map.entry(DocIdEncodingBenchmark1.DocIdEncoder.Bit21With3StepsEncoderOnlyRWLongEncoder.class, 21),
                Map.entry(DocIdEncodingBenchmark1.DocIdEncoder.Bit21HybridEncoder.class, 21),
                Map.entry(DocIdEncodingBenchmark1.DocIdEncoder.Bit24Encoder.class, 24),
                Map.entry(DocIdEncodingBenchmark1.DocIdEncoder.Bit32Encoder.class, 32),
                Map.entry(DocIdEncoder.Bit32OnlyRWLongEncoder.class, 32),
                Map.entry(DocIdEncoder.RoaringBitmapEncoder1.class, 32),
                Map.entry(DocIdEncoder.RoaringBitmapEncoderPacked.class, 32)
                //Map.entry(DocIdEncoder.DeltaDocIdEncoder.class, 16)
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

    static class FixedBPVRandomDocIdProvider implements DocIdEncodingBenchmark1.DocIdProvider {

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
                .include(DocIdEncodingBenchmark1.class.getSimpleName())
                .forks(1)
                .warmupIterations(1)
                .warmupTime(TimeValue.seconds(5))
                .measurementIterations(2)
                .measurementTime(TimeValue.seconds(8))
                .threads(1)
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .build();

        new Runner(opt).run();
    }
}