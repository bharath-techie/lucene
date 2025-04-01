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
package org.apache.lucene.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Simple implementation of {@link ChecksumIndexInput} that wraps another input and delegates calls.
 */
public class BufferedChecksumIndexInput extends ChecksumIndexInput {
  final IndexInput main;
  final BufferedChecksum digest;
  //final BufferedChecksum digest;
  // Add tracking variables
  Map<String, Long> map = new HashMap<>();
  Map<String, Long> totalBytesMap = new HashMap<>();
  Map<String, Long> totalTimeMap = new HashMap<>();
  Map<String, Long> invocationsMap = new HashMap<>();

  /** Creates a new BufferedChecksumIndexInput */
  public BufferedChecksumIndexInput(IndexInput main) {
    super("BufferedChecksumIndexInput(" + main + ")");
    this.main = main;
    this.digest = new BufferedChecksum(new CRC32());
  }

  @Override
  public byte readByte() throws IOException {
    final byte b = main.readByte();
    digest.update(b);
    return b;
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    String className = main.getClass().getName();

    // Track invocations and bytes
    invocationsMap.put(className, invocationsMap.getOrDefault(className, 0L) + 1);
    totalBytesMap.put(className, totalBytesMap.getOrDefault(className, 0L) + len);

    // Track timing
    long startTime = System.nanoTime();
    main.readBytes(b, offset, len);
    long endTime = System.nanoTime();
    digest.update(b, offset, len);;
    long timeSpent = endTime - startTime;

    totalTimeMap.put(className, totalTimeMap.getOrDefault(className, 0L) + timeSpent);
  }

  public void printStats() {
    long totalInvocations = invocationsMap.values().stream().mapToLong(Long::longValue).sum();
    long totalBytes = totalBytesMap.values().stream().mapToLong(Long::longValue).sum();

    System.out.println("Overall Statistics:");
    System.out.println("Total invocations across all classes: " + totalInvocations);
    System.out.println("Total bytes across all classes: " + totalBytes);
    System.out.println("Average length across all classes: " +
            (totalInvocations > 0 ? totalBytes / totalInvocations : 0));

    System.out.println("\nPer-class statistics:");
    for (Map.Entry<String, Long> entry : invocationsMap.entrySet()) {
      String className = entry.getKey();
      Long invocations = entry.getValue();
      Long bytes = totalBytesMap.get(className);
      Long totalTimeNanos = totalTimeMap.get(className);

      System.out.println("\nClass: " + className);
      System.out.println("  Invocations: " + invocations);
      System.out.println("  Total bytes: " + bytes);
      System.out.println("  Average bytes per invocation: " +
              (invocations > 0 ? bytes / invocations : 0));
      System.out.println("  Total time (ms): " + (totalTimeNanos / 1_000_000.0));
      System.out.println("  Average time per invocation (ms): " +
              (invocations > 0 ? (totalTimeNanos / 1_000_000.0) / invocations : 0));
      System.out.println("  Throughput (MB/s): " +
              (totalTimeNanos > 0 ? (bytes / 1024.0 / 1024.0) / (totalTimeNanos / 1_000_000_000.0) : 0));
    }
  }

  public void resetStats() {
    totalBytesMap = new HashMap<>();
    invocationsMap.clear();
    totalBytesMap.clear();
    totalTimeMap.clear();
  }

  @Override
  public short readShort() throws IOException {
    short v = main.readShort();
    digest.updateShort(v);
    return v;
  }

  @Override
  public int readInt() throws IOException {
    int v = main.readInt();
    digest.updateInt(v);
    return v;
  }

  @Override
  public long readLong() throws IOException {
    long v = main.readLong();
    digest.updateLong(v);
    return v;
  }

  @Override
  public void readLongs(long[] dst, int offset, int length) throws IOException {
    main.readLongs(dst, offset, length);
    digest.updateLongs(dst, offset, length);
  }

  @Override
  public long getChecksum() {
    return digest.getValue();
  }

  @Override
  public void close() throws IOException {
    main.close();
  }

  @Override
  public long getFilePointer() {
    return main.getFilePointer();
  }

  @Override
  public long length() {
    return main.length();
  }

  @Override
  public IndexInput clone() {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    throw new UnsupportedOperationException();
  }
}
