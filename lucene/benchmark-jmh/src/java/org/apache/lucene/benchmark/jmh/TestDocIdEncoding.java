package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Arrays;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.ByteBuffersDirectory;  // Changed this import
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;

public class TestDocIdEncoding extends LuceneTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testBPV21AndAbove() throws IOException {
        List<DocIdEncodingBenchmark1.DocIdEncoder> encoders =
                DocIdEncodingBenchmark1.DocIdEncoder.SingletonFactory.getAllExcept(Collections.emptyList());

        final int[] scratch = new int[512];

        DocIdEncodingBenchmark1.DocIdProvider docIdProvider =
                new DocIdEncodingBenchmark1.FixedBPVRandomDocIdProvider();

        // Use ByteBuffersDirectory instead of FSDirectory
        Directory directory = new ByteBuffersDirectory();

        try {
            for (DocIdEncodingBenchmark1.DocIdEncoder encoder : encoders) {
                List<int[]> docIdSequences = docIdProvider.getDocIds(encoder.getClass(), 100, 100, 512);

                String encoderFileName = "Encoder_" + encoder.getClass().getSimpleName();

                try (IndexOutput out = directory.createOutput(encoderFileName, IOContext.DEFAULT)) {
                    for (int[] sequence : docIdSequences) {
                        encoder.encode(out, 0, sequence.length, sequence);
                    }
                }

                try (IndexInput in = directory.openInput(encoderFileName, IOContext.DEFAULT)) {
                    for (int[] sequence : docIdSequences) {
                        encoder.decode(in, 0, sequence.length, scratch);
                        int[] decodedArray = ArrayUtil.copyOfSubArray(scratch, 0, sequence.length);
                        assert Arrays.equals(sequence, decodedArray) :
                                encoderFileName + " : Arrays are not equal. Expected: " + Arrays.toString(sequence) +
                                        " but was: " + Arrays.toString(decodedArray);
                    }
                } finally {
                    directory.deleteFile(encoderFileName);
                }
            }
        } finally {
            directory.close();
        }
    }
}