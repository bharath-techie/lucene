package org.apache.lucene.tests;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.BeforeClass;


public class TestPoints extends LuceneTestCase {
    private static File plaintextDir;
    private static File mixedDir;
    private static File plaintextDir4;

    @BeforeClass
    public static void setUpDirectories() {
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        plaintextDir = assureDirectoryExists(new File(tmpDir, "lucene-plaintext-18"));
        plaintextDir4 = assureDirectoryExists(new File(tmpDir, "lucene-plaintext-9"));
        mixedDir = assureDirectoryExists(new File(tmpDir, "lucene-mixed"));
    }

    private static File assureDirectoryExists(File dir) {
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return dir;
    }
    private static int getRandomDay() {
        int minDay = 1;
        int maxDay = 31;
        Random random = new Random();
        return random.nextInt(maxDay - minDay + 1) + minDay;
    }

    private static int getRandomStatus() {
        int[] statusCodes = {200, 201, 202, 300, 301, 302, 400, 401, 403, 404, 500};
        Random random = new Random();
        return statusCodes[random.nextInt(statusCodes.length)];
    }
    private static int getRandomStatus200() {
        int[] statusCodes = {200, 200, 200, 200, 200, 200, 200, 200, 200, 404, 500};
        Random random = new Random();
        return statusCodes[random.nextInt(statusCodes.length)];
    }

    private static int getPort() {
        int minHour = 10000;
        int maxHour = 20000;
        Random random = new Random();
        return random.nextInt(maxHour - minHour + 1) + minHour;
    }

    private static int getRandomHour() {
        int minHour = 1000000;
        int maxHour = 2000000;
        Random random = new Random();
        return random.nextInt(maxHour - minHour + 1) + minHour;
    }

    private static int getRandomHour1() {
        int minHour = 0;
        int maxHour = 23;
        Random random = new Random();
        return random.nextInt(maxHour - minHour + 1) + minHour;
    }

    public void test2dPoints2()
            throws Exception {
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setUseCompoundFile(false);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setCodec(new Lucene101Codec());
        Directory luceneDir = FSDirectory.open(plaintextDir.toPath());
        System.out.println("Dir : " + plaintextDir.toPath());
        IndexWriter w = new IndexWriter(luceneDir, config);

        Map<Integer, Integer> statusToCountMap = new HashMap<>();
        Map<Integer, Integer> hourToCountMap = new HashMap<>();


        int totalDocs = 1000000;
        int docsAdded = 0;
        int total = 0;
        while (docsAdded < totalDocs) {

            int hour = getRandomHour();
            int status = getRandomStatus200();
            int port = getPort();
            for (int i = 0; i < 100; i++) {
                statusToCountMap.put(status, statusToCountMap.getOrDefault(status, 0) + 1);
                hourToCountMap.put(hour, hourToCountMap.getOrDefault(hour, 0) + 1);

                if(status==200 && hour > 1800000) {
                    total++;
                }
                Document doc = new Document();
                doc.add(new IntPoint("timestamp-status", hour, hour/24, status));
                doc.add(new SortedNumericDocValuesField("status", status));
                w.addDocument(doc);
            }

            docsAdded += 100;
        }

        w.flush();
        System.out.println("Expected : " + total);
        w.forceMerge(1);
        //w.commit();
        queryStatusHourPoints(w);
    }

    private void queryPoints(IndexWriter w)
            throws IOException {
        long startTime = System.currentTimeMillis();

        IndexReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader, false);

        System.out.println("=== Hour is the dimension with largest span, day with second largest,"
                + "status has super small range");

        // Search for a specific status
        int[] min = {0,0,500};
        int[] max = {2500000,2500000,500};


        Query query =
                IntPoint.newRangeQuery("timestamp-status", min, max);
        startTime = System.nanoTime();
        Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        // One query has a count of 0, the disjunction count is the other count
        //assertEquals(1, weight.count(reader.leaves().get(0)));

        System.out.println("Count : " + weight.count(reader.leaves().get(0)));

        System.out.println("===== Finished querying point-tree in ms => status = 500 : " +
                (System.nanoTime() - startTime)/1000000 + " ms");

        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader, false);

        min = new int[]{1800000,0,500};
        max = new int[]{2500000,2500000,500};

        query =
                IntPoint.newRangeQuery("timestamp-status", min, max);
        startTime = System.nanoTime();
        weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        System.out.println("Count : " + weight.count(reader.leaves().get(0)));
        System.out.println("===== Finished querying point-tree in ms => status = 500 + hour range : " +
                (System.nanoTime() - startTime)/1000000 + " ms");

        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader, false);

        min = new int[]{1800000,0,0};
        max = new int[]{2500000,2500000,500};

        query =
                IntPoint.newRangeQuery("timestamp-status", min, max);
        startTime = System.nanoTime();
        weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        System.out.println("Count : " + weight.count(reader.leaves().get(0)));
        System.out.println("===== Finished querying point-tree in ms => hour range : " +
                (System.nanoTime() - startTime)/1000000 + " ms");

        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader, false);

        min = new int[]{0,1800000/24,0};
        max = new int[]{2500000,2500000,500};

        query =
                IntPoint.newRangeQuery("timestamp-status", min, max);
        startTime = System.nanoTime();
        weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        System.out.println("Count : " + weight.count(reader.leaves().get(0)));
        System.out.println("===== Finished querying point-tree in ms => day range : " +
                (System.nanoTime() - startTime)/1000000 + " ms");

        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader, false);

        min = new int[]{0,1800000/24,500};
        max = new int[]{2500000,2500000,500};
        query =
                IntPoint.newRangeQuery("timestamp-status", min, max);
        startTime = System.nanoTime();
        weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        System.out.println("Count : " + weight.count(reader.leaves().get(0)));
        System.out.println("===== Finished querying point-tree in ms => 500 + day range : " +
                (System.nanoTime() - startTime)/1000000 + " ms");
    }

    private void queryStatusHourPoints(IndexWriter w)
            throws IOException {
        long startTime = System.currentTimeMillis();

        IndexReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader, false);


        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader, false);

        int[] min = new int[]{1800000,0,500};
        int[] max = new int[]{2500000,2500000,500};

        Query query =
                IntPoint.newRangeQuery("timestamp-status", min, max);
        startTime = System.nanoTime();
        Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        System.out.println("Count : " + weight.count(reader.leaves().get(0)));
        System.out.println("===== Finished querying point-tree in ms => status = 500 + hour range : " +
                (System.nanoTime() - startTime)/1000000 + " ms");
    }

    public void testRangeBitMap()
            throws Exception {
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setUseCompoundFile(false);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setCodec(new Lucene101Codec());
        Directory luceneDir = FSDirectory.open(plaintextDir.toPath());
        System.out.println("Dir : " + plaintextDir.toPath());
        IndexWriter w = new IndexWriter(luceneDir, config);

        Map<Integer, Integer> statusToCountMap = new HashMap<>();
        Map<Integer, Integer> hourToCountMap = new HashMap<>();


        int totalDocs = 1000000;
        int docsAdded = 0;
        int total = 0;
        while (docsAdded < totalDocs) {

            int hour = getRandomHour();
            for (int i = 0; i < 100; i++) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("@timestamp", hour));
                w.addDocument(doc);
            }
            docsAdded += 100;
        }

        w.flush();
        System.out.println("Expected : " + total);
        w.forceMerge(1);
        //w.commit();
        queryStatusSortedNumericDocValues(w);
    }

    private void queryStatusSortedNumericDocValues(IndexWriter w)
            throws IOException {
        long startTime = System.currentTimeMillis();

        IndexReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader, false);


        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader, false);

        int[] min = new int[]{1800000,0,500};
        int[] max = new int[]{2500000,2500000,500};

        Query query =
                IntPoint.newRangeQuery("timestamp-status", min, max);
        startTime = System.nanoTime();
        Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        //weight.scorer()
        System.out.println("Count : " + weight.count(reader.leaves().get(0)));
        System.out.println("===== Finished querying point-tree in ms => status = 500 + hour range : " +
                (System.nanoTime() - startTime)/1000000 + " ms");
    }

    public static class SimpleLeafCollector implements LeafCollector {
        private int count = 0;

        @Override
        public void setScorer(Scorable scorer) {}

        @Override
        public void collect(int doc) {
            count++;
        }

        public int getCount() {
            return count;
        }
    }

//    public void queryStatus200DocValues(IndexSearcher searcher) throws IOException {
//        Query query = SortedNumericDocValuesField.newSlowRangeQuery("status_dv", 200, 200);
//        query(searcher, query);
//    }

//    private void query(IndexSearcher searcher, IndexReader reader, Query query) throws IOException {
//        Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
//        BulkScorer bulkScorer = weight.bulkScorer(reader.leaves().get(0));
//        collector = new SimpleLeafCollector();
//        bulkScorer.score(collector, liveDocs, 0, reader.maxDoc());
//    }

    private void queryStatusPoints(IndexWriter w)
            throws IOException {
        long startTime = System.currentTimeMillis();

        IndexReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader, false);


        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader, false);

        int[] min = new int[]{0,0,500};
        int[] max = new int[]{2500000,2500000,500};

        Query query =
                IntPoint.newRangeQuery("timestamp-status", min, max);
        startTime = System.nanoTime();
        Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        System.out.println("Count : " + weight.count(reader.leaves().get(0)));
        System.out.println("===== Finished querying point-tree in ms => status = 500 : " +
                (System.nanoTime() - startTime)/1000000 + " ms");
    }

    private void queryHourRange(IndexWriter w)
            throws IOException {
        long startTime = System.currentTimeMillis();

        IndexReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader, false);


        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader, false);

        int[] min = new int[]{1800000,0,0};
        int[] max = new int[]{2500000,2500000,500};

        Query query =
                IntPoint.newRangeQuery("timestamp-status", min, max);
        startTime = System.nanoTime();
        Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        System.out.println("Count : " + weight.count(reader.leaves().get(0)));
        double millis = (System.nanoTime() - startTime) * 1.0 / 1000000.0;
        System.out.println("===== Finished querying point-tree in ms => hour range : " +
                millis + " ms");
    }

    private void queryDayRange(IndexWriter w)
            throws IOException {
        long startTime = System.currentTimeMillis();

        IndexReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader, false);


        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader, false);

        int[] min = new int[]{0,1800000/24,0};
        int[] max = new int[]{2500000,2500000,500};

        Query query =
                IntPoint.newRangeQuery("timestamp-status", min, max);
        startTime = System.nanoTime();
        Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        System.out.println("Count : " + weight.count(reader.leaves().get(0)));
        double millis = (System.nanoTime() - startTime) * 1.0 / 1000000.0;
        System.out.println("===== Finished querying point-tree in ms => day range : " +
                millis + " ms");
    }

    private void queryDayRangeWithStatus(IndexWriter w)
            throws IOException {
        long startTime = System.currentTimeMillis();

        IndexReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader, false);


        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader, false);

        int[] min = new int[]{0,1800000/24,500};
        int[] max = new int[]{2500000,2500000,500};

        Query query =
                IntPoint.newRangeQuery("timestamp-status", min, max);
        startTime = System.nanoTime();
        Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        System.out.println("Count : " + weight.count(reader.leaves().get(0)));
        double millis = (System.nanoTime() - startTime) * 1.0 / 1000000.0;
        System.out.println("===== Finished querying point-tree in ms => day range + status : " +
                millis + " ms");
    }

    private void queryDayRangeWithHourDayStatus(IndexWriter w)
            throws IOException {
        long startTime = System.currentTimeMillis();

        IndexReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader, false);


        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader, false);

        int[] min = new int[]{1800000,1800000/24,500};
        int[] max = new int[]{2500000,2500000,500};

        Query query =
                IntPoint.newRangeQuery("timestamp-status", min, max);
        startTime = System.nanoTime();
        Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        System.out.println("Count : " + weight.count(reader.leaves().get(0)));
        double millis = (System.nanoTime() - startTime) * 1.0 / 1000000.0;
        System.out.println("===== Finished querying point-tree in ms => day range + status : " +
                millis + " ms");
    }

    public void test2dPoints3() throws Exception {
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setUseCompoundFile(false);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setCodec(new SimpleTextCodec());
        Directory luceneDir = FSDirectory.open(plaintextDir.toPath());
        System.out.println("Dir : " + plaintextDir.toPath());
        IndexWriter w = new IndexWriter(luceneDir, config);

        Map<Integer, Integer> statusToCountMap = new HashMap<>();
        Map<Integer, Integer> hourToCountMap = new HashMap<>();

        int totalDocs = 10000;
        int docsAdded = 0;
        int total = 0;
        while (docsAdded < totalDocs) {
            int hour = getRandomHour1();
            int status = getRandomStatus200();
            int port = getPort();
            Document doc = new Document();
          //  doc.add(new IntPoint("timestamp-status", hour, port, status));
           //doc.add(new IntPoint("status", status));
            doc.add(new IntPoint("port", port));
//            doc.add(new IntPoint("hour", hour));
            w.addDocument(doc);
            /**
            for (int i = 0; i < 100; i++) {
                statusToCountMap.put(status, statusToCountMap.getOrDefault(status, 0) + 1);
                hourToCountMap.put(hour, hourToCountMap.getOrDefault(hour, 0) + 1);

                if(status==200 && hour > 18) {
                    total++;
                }
                Document doc = new Document();
                // Index as 3D point
                //doc.add(new IntPoint("timestamp-status", hour, port, status));

                doc.add(new IntPoint("status", status));
                doc.add(new IntPoint("port", port));
                doc.add(new IntPoint("hour", hour));

                // Index as DocValues
//                doc.add(new SortedNumericDocValuesField("hour_dv", hour));
//                doc.add(new SortedNumericDocValuesField("status_dv", status));
//                doc.add(new SortedNumericDocValuesField("port_dv", port));

                w.addDocument(doc);
            }
             **/
            docsAdded += 1;
        }

        w.flush();
        System.out.println("Expected : " + total);
        w.forceMerge(1);

        compareQueries(w);
    }

    private void compareQueries(IndexWriter w) throws IOException {
        IndexReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader, false);

        // Query 1: status = 200
        System.out.println("\n=== Query 1: status = 200 ===");
        int pointCount4 = 0;
        long startTime = System.nanoTime();
        Query pointQuery = IntPoint.newRangeQuery("status", 200, 200);
        Weight weight2 = searcher.createWeight(pointQuery, ScoreMode.COMPLETE, 1f);
        BulkScorer bs = weight2.bulkScorer(reader.leaves().get(0));

        Scorer scorer2 = weight2.scorer(reader.leaves().get(0));
        DocIdSetIterator it2 = scorer2.iterator();
        while (it2.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            pointCount4++;
        }
        long pointQueryTime4 = System.nanoTime() - startTime;
        System.out.println("Single field Time: " + pointQueryTime4/1_000_000.0 + " ms");
        // Points Query
        startTime = System.nanoTime();
        int[] min1 = new int[]{0, 0, 200};
        int[] max1 = new int[]{23, 20000, 200};
        Query pointQuery1 = IntPoint.newRangeQuery("timestamp-status", min1, max1);
        Weight weight = searcher.createWeight(pointQuery1, ScoreMode.COMPLETE, 1f);

        int pointCount1 = 0;
        Scorer scorer = weight.scorer(reader.leaves().get(0));
        DocIdSetIterator it = scorer.iterator();
        while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            pointCount1++;
        }
        long pointQueryTime1 = System.nanoTime() - startTime;

        // DocValues Query
        startTime = System.nanoTime();
        Query docValuesQuery1 = SortedNumericDocValuesField.newSlowRangeQuery("status_dv", 200, 200);
        weight = searcher.createWeight(docValuesQuery1, ScoreMode.COMPLETE, 1f);

        int docValueCount1 = 0;
        scorer = weight.scorer(reader.leaves().get(0));
        it = scorer.iterator();
        while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            docValueCount1++;
        }
        long docValueQueryTime1 = System.nanoTime() - startTime;

        System.out.println("Points Query:");
        System.out.println("Count: " + pointCount1);
        System.out.println("Time: " + pointQueryTime1/1_000_000.0 + " ms");

        System.out.println("\nDocValues Query:");
        System.out.println("Count: " + docValueCount1);
        System.out.println("Time: " + docValueQueryTime1/1_000_000.0 + " ms");

        // Query 2: status = 500 && port = 15000
        System.out.println("\n=== Query 2: status = 500 && port = 15000 ===");

        // Points Query
        startTime = System.nanoTime();
        int[] min2 = new int[]{0, 14000, 500};
        int[] max2 = new int[]{23, 15000, 500};
        Query pointQuery2 = IntPoint.newRangeQuery("timestamp-status", min2, max2);
        weight = searcher.createWeight(pointQuery2, ScoreMode.COMPLETE, 1f);

        int pointCount2 = 0;
        scorer = weight.scorer(reader.leaves().get(0));
        long pointQueryTime2 = System.nanoTime() - startTime;
        it = scorer.iterator();
        while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            pointCount2++;
        }


        // DocValues Query
        startTime = System.nanoTime();
        Query statusQuery = SortedNumericDocValuesField.newSlowRangeQuery("status_dv", 500, 500);
        Query portQuery = SortedNumericDocValuesField.newSlowRangeQuery("port_dv", 15000, 15000);

        BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
        bqBuilder.add(statusQuery, BooleanClause.Occur.MUST);
        bqBuilder.add(portQuery, BooleanClause.Occur.MUST);
        Query docValuesQuery2 = bqBuilder.build();

        weight = searcher.createWeight(docValuesQuery2, ScoreMode.COMPLETE, 1f);

        int docValueCount2 = 0;
        scorer = weight.scorer(reader.leaves().get(0));
        it = scorer.iterator();
        while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            docValueCount2++;
        }
        long docValueQueryTime2 = System.nanoTime() - startTime;

        System.out.println("Points Query:");
        System.out.println("Count: " + pointCount2);
        System.out.println("Time: " + pointQueryTime2/1_000_000.0 + " ms");

        System.out.println("\nDocValues Query:");
        System.out.println("Count: " + docValueCount2);
        System.out.println("Time: " + docValueQueryTime2/1_000_000.0 + " ms");
    }
}
