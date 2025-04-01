package org.apache.lucene.util.bkd;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class TestQueries extends LuceneTestCase {

    private IndexWriter writer;
    private IndexReader reader;
    private IndexSearcher searcher;
    private Directory directory;
    private int numDocs = 100000;
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

        directory = newFSDirectory(createTempDir("TestBKDTree"));
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setCodec(new Lucene101Codec());
        writer = new IndexWriter(directory, config);
        int cardinality = 100000;
        int statuscardinality = 100;
        int stringCardinality = 10;
        Random random = new Random();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            int hour = random().nextInt(cardinality);
            if(min < hour) {
                min = hour;
            }
//            doc.add(new IntPoint("timestamp", hour));
//            doc.add(new SortedNumericDocValuesField("@timestamp", hour));
            doc.add(new SortedNumericDocValuesField("status", random.nextInt(statuscardinality)));
            doc.add(new IntPoint("status", random.nextInt(statuscardinality)));
            doc.add(new TextField("text", "a"+random.nextInt(stringCardinality), Field.Store.NO));

            writer.addDocument(doc);
        }
        writer.forceMerge(1);
        writer.commit();
        reader = DirectoryReader.open(writer);
        searcher = new IndexSearcher(reader);
        searcher.setQueryCachingPolicy(
                new QueryCachingPolicy() {
                    @Override
                    public void onUse(Query query) {}

                    @Override
                    public boolean shouldCache(Query query) throws IOException {
                        return false;
                    }
                });
        liveDocs = new Bits.MatchAllBits(reader.maxDoc());
    }

    public void testIndexAndDocValuesQuery() throws Exception {
        // Query 1: Boolean query with term query on string field and IndexOrDocValuesQuery on status field
        BooleanQuery.Builder queryBuilder1 = new BooleanQuery.Builder();
        queryBuilder1.add(new TermQuery(new Term("text", "a50")), BooleanClause.Occur.MUST); // Example term query
        queryBuilder1.add(new IndexOrDocValuesQuery(IntPoint.newExactQuery("status", 50),
                SortedNumericDocValuesField.newSlowExactQuery("status", 50)), BooleanClause.Occur.MUST); // Example IndexOrDocValuesQuery
        doSearch(searcher, queryBuilder1.build());
//        System.out.println("================ second query =================");
//        // Query 2: Boolean query with term query on status field and term query on timestamp field
        Query q = new IndexOrDocValuesQuery(IntPoint.newExactQuery("status", 50),
                SortedNumericDocValuesField.newSlowExactQuery("status", 50));
        doSearch(searcher, q);
        // Query 3: Boolean query with term query on status field and term query on timestamp field
        Query q1 = SortedNumericDocValuesField.newSlowExactQuery("status", 50);
        doSearch(searcher, q1);
        writer.close();
        reader.close();
        directory.close();
    }

    private static void doSearch(IndexSearcher searcher, Query query) throws IOException {
        AtomicInteger a = new AtomicInteger();
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
                                a.incrementAndGet();
                            }
                        };
                    }

                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }
                });

        System.out.println("count : "  +a.get());
    }

    private void doSearchWithTopHits(IndexSearcher searcher, Query query) throws IOException {
        TotalHitCountCollector collector = new TotalHitCountCollector();
        searcher.search(query, collector);
        System.out.println("Query: " + query + ", Count: " + collector.getTotalHits());
    }

}
