package org.apache.lucene.tests;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class TestBKD1 extends LuceneTestCase {
    private IndexWriter writer;
    private IndexReader reader;
    private IndexSearcher searcher;
    private Directory directory;
    private int numDocs = 10000;
    private Bits liveDocs;
    int min = Integer.MAX_VALUE;
    private static File plaintextDir;
    private static final String FIELD = "field";
    private static final boolean BASELINE = false;
    private static final boolean INDEX = true;
    private static final boolean SEARCH = true;
    private static final Random RANDOM = new Random(578136438746182L);

    @Before
    public void setup() throws IOException {


//        Analyzer analyzer = new StandardAnalyzer();
//        IndexWriterConfig config = new IndexWriterConfig(analyzer);
//        config.setCodec(new Lucene101Codec());
//        writer = new IndexWriter(directory, config);

        // Index documents
//        Random random = new Random();
//        for (int i = 0; i < numDocs; i++) {
//            Document doc = new Document();
//            int hour = 1;//getRandomHour();
//            doc.add(new IntPoint("timestamp", hour));
//            if(min < hour) {
//                min = hour;
//            }
//            doc.add(new SortedNumericDocValuesField("@timestamp", hour));
//
//            writer.addDocument(doc);
//        }
//
//        writer.commit();
//        writer.forceMerge(1);
//        reader = DirectoryReader.open(writer);
//        searcher = new IndexSearcher(reader);
//        // Setup collector that just counts matches
//        collector = new TestRangeBitMap.SimpleLeafCollector();
//        liveDocs = new Bits.MatchAllBits(reader.maxDoc());
    }

    public void test2dPoints() throws IOException {
        index(32, 100000000);
        search(32, 100000000, 1);
        search(32, 100000000, 2);
        search(32, 100000000, 4);
        search(32, 100000000, 8);
        search(32, 100000000, 16);

        index(128, 100000000);
        search(128, 100000000, 1);
        search(128, 100000000, 8);
        search(128, 100000000, 16);
        search(128, 100000000, 32);
        search(128, 100000000, 64);

        index(1024, 100000000);
        search(1024, 100000000, 1);
        search(1024, 100000000, 8);
        search(1024, 100000000, 32);
        search(1024, 100000000, 128);
        search(1024, 100000000, 512);

        index(8192, 100000000);
        search(8192, 100000000, 1);
        search(8192, 100000000, 16);
        search(8192, 100000000, 64);
        search(8192, 100000000, 512);
        search(8192, 100000000, 2048);

        index(1048576, 100000000);
        search(1048576, 100000000, 1);
        search(1048576, 100000000, 16);
        search(1048576, 100000000, 64);
        search(1048576, 100000000, 512);
        search(1048576, 100000000, 2048);
//
//    index(1024 * 1024, 100000000);
//    search(1024 * 1024, 100000000, 1);
//    search(1024 * 1024, 100000000, 16);
//    search(1024 * 1024, 100000000, 64);
//    search(1024 * 1024, 100000000, 512);
//    search(1024 * 1024, 100000000, 2048);
    }

    private static void index(int cardinality, int docCount) throws IOException {
        if (INDEX == false) {
            return;
        }
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        plaintextDir = assureDirectoryExists(new File(tmpDir, "lucene-plaintext-18" + dictName(cardinality, docCount)));

        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setUseCompoundFile(false);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setCodec(new Lucene101Codec());
        Directory luceneDir = FSDirectory.open(plaintextDir.toPath());
        System.out.println("Dir : " + plaintextDir.toPath());
        //Directory directory = FSDirectory.open(Paths.get("./" + dictName(cardinality, docCount)));
//        IndexWriter writer =
//                new IndexWriter(
//                        directory, new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE));

        IndexWriter writer = new IndexWriter(luceneDir, config);
        for (int i = 0; i < docCount; i++) {
            Document document = new Document();
            long point = RANDOM.nextInt(cardinality);
            document.add(new LongPoint(FIELD, point));
            writer.addDocument(document);
        }
        writer.flush();
        writer.commit();
        //writer.forceMerge(1);
        writer.close();
    }

//    private static String dictName(int cardinality, int docCount) throws IOException {
//        return "roaring_2long_optimized_index_"
//        //return "bpv_21_index_"
//                + docCount
//                + "_doc_"
//                + cardinality
//                + "_cardinality_"
//                + (BASELINE ? "baseline" : "candidate");
 //   }

    private static String dictName(int cardinality, int docCount) throws IOException {
        return "baseline_index_"
                + docCount
                + "_doc_"
                + cardinality
                + "_cardinality_"
                + (BASELINE ? "baseline" : "candidate");
    }

    private static void search(int cardinality, int docCount, int termCount) throws IOException {
        if (SEARCH == false) {
            return;
        }

        String fileName = dictName(cardinality, docCount);
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        plaintextDir = new File(tmpDir, "lucene-plaintext-18" + dictName(cardinality, docCount));
        Directory directory = FSDirectory.open(plaintextDir.toPath());
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        searcher.setQueryCachingPolicy(
                new QueryCachingPolicy() {
                    @Override
                    public void onUse(Query query) {}

                    @Override
                    public boolean shouldCache(Query query) throws IOException {
                        return false;
                    }
                });
        long total = 0;
        for (int i = 0; i < 100; i++) {
            Query query = getQuery(cardinality, termCount);
            long start = System.currentTimeMillis();
            doSearch(searcher, query);
            long end = System.currentTimeMillis();
            total += end - start;
        }
        System.out.println(
                "task: " + fileName + ", term count: " + termCount + ", took: " + total / 100);
    }

    private static Query getQuery(int cardinality, int termCount) {
        assert termCount < cardinality;
        Set<Integer> set = new HashSet<>(cardinality);
        while (set.size() < termCount) {
            set.add(RANDOM.nextInt(cardinality));
        }
        long[] terms = new long[termCount];
        int pos = 0;
        for (long l : set) {
            terms[pos++] = l;
        }
        return LongPoint.newSetQuery(FIELD, terms);
    }

    private static void doSearch(IndexSearcher searcher, Query query) throws IOException {
        searcher.search(
                query,
                new Collector() {
                    @Override
                    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                        return new LeafCollector() {
                            @Override
                            public void setScorer(Scorable scorer) throws IOException {}

                            @Override
                            public void collect(int doc) throws IOException {
                                throw new CollectionTerminatedException();
                            }
                        };
                    }

                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }
                });
    }

    private static File assureDirectoryExists(File dir) {
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return dir;
    }
}
