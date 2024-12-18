package org.apache.lucene.tests;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.NumericUtils;
import org.junit.Before;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class TestRangeBitMapFloat extends LuceneTestCase {
    private IndexWriter writer;
    private IndexReader reader;
    private IndexSearcher searcher;
    private Directory directory;
    private int numDocs = 10000000;
    private SimpleLeafCollector collector;
    private Bits liveDocs;
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    private static File plaintextDir;
    @Before
    public void setup() throws IOException {
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        File indexDir = new File(tmpDir, "lucene-points-benchmark-1");
        plaintextDir = assureDirectoryExists(new File(tmpDir, "lucene-plaintext-18"));
        if (!indexDir.exists()) {
            indexDir.mkdirs();
        }

       Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setUseCompoundFile(false);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setCodec(new Lucene101Codec());
        Directory luceneDir = FSDirectory.open(plaintextDir.toPath());
        System.out.println("Dir : " + plaintextDir.toPath());
        IndexWriter writer = new IndexWriter(luceneDir, config);
        directory = FSDirectory.open(indexDir.toPath());

//        Analyzer analyzer = new StandardAnalyzer();
//        IndexWriterConfig config = new IndexWriterConfig(analyzer);
//        config.setCodec(new Lucene101Codec());
//        writer = new IndexWriter(directory, config);

        // Index documents
        Random random = new Random();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            float hour = getRandomHour();
            int longHour = NumericUtils.floatToSortableInt(hour);
            doc.add(new FloatPoint("timestamp", hour));

            // Index as DocValues
            //doc.add(new SortedNumericDocValuesField("status_dv", status));
            if((NumericUtils.sortableIntToFloat(longHour) < NumericUtils.sortableIntToFloat(min) || min == Integer.MAX_VALUE)) {
                min = longHour;
            }
            if((NumericUtils.sortableIntToFloat(longHour) > NumericUtils.sortableIntToFloat(max)) || max == Integer.MIN_VALUE) {
                max = longHour;
            }
            doc.add(new SortedNumericDocValuesField("@timestamp", longHour));
            //doc.add(new SortedNumericDocValuesField("port_dv", port));

            writer.addDocument(doc);
        }

        writer.commit();
        writer.forceMerge(1);
        reader = DirectoryReader.open(writer);
        searcher = new IndexSearcher(reader);
        // Setup collector that just counts matches
        collector = new SimpleLeafCollector();
        liveDocs = new Bits.MatchAllBits(reader.maxDoc());
    }

    private static float getRandomHour() {
        int minHour = 10000;
        int maxHour = 50000;
        Random random = new Random();
        return random.nextFloat(maxHour - minHour + 1) + minHour;
        //return maxHour + (maxHour - minHour) * random.nextDouble();
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
    private static File assureDirectoryExists(File dir) {
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return dir;
    }

    public void test2dPoints2()
            throws Exception {
        long st = System.currentTimeMillis();
        int min = NumericUtils.floatToSortableInt(10000);
        int max = NumericUtils.floatToSortableInt(10100);
        Query query = SortedNumericDocValuesField.newSlowRangeQuery("@timestamp", min, max);
        query(query);
        System.out.println("Time taken for DV : " + (System.currentTimeMillis() - st));

        st = System.currentTimeMillis();
        pointsQuery(query, 10000, 10100);
        System.out.println("Time taken for Points : " + (System.currentTimeMillis() - st));

        st = System.currentTimeMillis();
        queryRangeBitmap(query, min, max);
        System.out.println("Time taken for RangeBitmap : " + (System.currentTimeMillis() - st));

        st = System.currentTimeMillis();
        min = NumericUtils.floatToSortableInt(10000);
        max = NumericUtils.floatToSortableInt(40000);
        query = SortedNumericDocValuesField.newSlowRangeQuery("@timestamp", min, max);
        query(query);
        System.out.println("Time taken for DV : " + (System.currentTimeMillis() - st));

        st = System.currentTimeMillis();
        pointsQuery(query, 10000, 40000);
        System.out.println("Time taken for Points : " + (System.currentTimeMillis() - st));

        st = System.currentTimeMillis();
        queryRangeBitmap(query, min, max);
        System.out.println("Time taken for RangeBitmap : " + (System.currentTimeMillis() - st));

    }

    private void query(Query query) throws IOException {
        Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        BulkScorer bulkScorer = weight.bulkScorer(reader.leaves().get(0));
        collector = new SimpleLeafCollector();
        bulkScorer.score(collector, liveDocs, 0, reader.maxDoc());
        System.out.println("DocValues : " + collector.count);
    }

    private void pointsQuery(Query query1, int min, int max) throws IOException {
        Query query = FloatPoint.newRangeQuery("timestamp", (float) min, (float) max);
        Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        BulkScorer bulkScorer = weight.bulkScorer(reader.leaves().get(0));
        collector = new SimpleLeafCollector();
        bulkScorer.score(collector, liveDocs, 0, reader.maxDoc());
        System.out.println("Points : " + collector.count);
    }

    private void queryRangeBitmap(Query query, long min, long max) throws IOException {
        collector = new SimpleLeafCollector();
        RangeBitmap r =  reader.leaves().get(0).reader().getRangeBitMap();
        RoaringBitmap r1 = r.between((min-this.min), (max-this.min));
        PeekableIntIterator a = r1.getIntIterator();

//        ImmutableRoaringBitmap r = queryRangeBitmap(min-this.min,max-this.min, this.max - this.min);
//        PeekableIntIterator a = r.getIntIterator();
        while (a.hasNext() ) {
            collector.collect(a.next());
        }
        System.out.println("Range bitmap : " + collector.count);
    }

    private ImmutableRoaringBitmap queryRangeBitmap(long min, long max, long columnMax) throws IOException {
        RangeBitmap rangeBitmap =  reader.leaves().get(0).reader().getRangeBitMap();
        return rangeBitmap.between(min, max).toMutableRoaringBitmap();

//        if (max < columnMax) {
//            if (min > 0) {
//                if (min == max) {
//                    return rangeBitmap.eq(min).toMutableRoaringBitmap();
//                }
//                return rangeBitmap.between(min, max).toMutableRoaringBitmap();
//            }
//            return rangeBitmap.lte(max).toMutableRoaringBitmap();
//        } else {
//            if (min > 0) {
//                return rangeBitmap.gte(min).toMutableRoaringBitmap();
//            }
//            MutableRoaringBitmap all = new MutableRoaringBitmap();
//            all.add(0L, reader.maxDoc());
//            return all;
//        }
    }


}
