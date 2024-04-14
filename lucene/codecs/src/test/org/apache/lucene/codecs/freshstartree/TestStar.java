package org.apache.lucene.codecs.freshstartree;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Predicate;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.freshstartree.codec.StarTreeAggregatedValues;
import org.apache.lucene.codecs.freshstartree.codec.StarTreeCodec;
import org.apache.lucene.codecs.freshstartree.query.StarTreeQuery;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.DocIdSetBuilder;
import org.junit.BeforeClass;


public class TestStar extends LuceneTestCase {
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
  public void testStarTree()
      throws Exception {
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setUseCompoundFile(false);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    config.setCodec(new StarTreeCodec());
    Directory luceneDir = FSDirectory.open(plaintextDir.toPath());
    System.out.println("Dir : " + plaintextDir.toPath());
    IndexWriter w = new IndexWriter(luceneDir, config);

    Map<Integer, Integer> statusToCountMap = new HashMap<>();
    Map<Integer, Integer> hourToCountMap = new HashMap<>();


    int totalDocs = 1000000;
    int docsAdded = 0;
    int total = 0;
    int sum = 0;
    while (docsAdded < totalDocs) {

      int hour = getRandomHour();
      int status = getRandomStatus200();

      for (int i = 0; i < 100; i++) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("hour", hour));
        if(hour >=1800000 && status ==200) {
          sum += status;
        }
        doc.add(new SortedNumericDocValuesField("day", hour/24));
        doc.add(new SortedNumericDocValuesField("status", status));
        w.addDocument(doc);
      }
      docsAdded += 100;
    }

    w.flush();
    System.out.println("Expected : " + total);
    w.forceMerge(1);
    //w.commit();
    queryStarTree(w);
  }

  private void queryStarTree(IndexWriter w)
      throws IOException {
    long startTime = System.currentTimeMillis();

    final IndexReader reader = DirectoryReader.open(w);
    final IndexSearcher searcher = newSearcher(reader, false);


    Map<String, List<Predicate<Integer>>> predicateMap = new HashMap<>();
    List<Predicate<Integer>> predicates = new ArrayList<>();
    predicates.add(day -> day >= 1800000 && day <= 2000000);
    predicateMap.put("hour", predicates);
    predicates = new ArrayList<>();
    predicates.add(status -> status == 200);
    predicateMap.put("status", predicates);


    final Query q = new StarTreeQuery(predicateMap, new HashSet<>());
    reader.leaves().get(0).reader().getAggregatedDocValues();
    searcher.search(q, getAggregationCollector());
    System.out.println("============== Finished querying star-tree in ms : " +
        (System.currentTimeMillis() - startTime));


    startTime = System.nanoTime();
    Weight weight = searcher.createWeight(q, ScoreMode.COMPLETE, 1f);
    System.out.println("Count : " + weight.count(reader.leaves().get(0)));

    startTime = System.currentTimeMillis();
//    final Query q1 = new MatchAllDocsQuery();
//    searcher.search(q1, getSumCollector());
//    System.out.println("Finished querying normal doc values in ms : " +
//        (System.currentTimeMillis() - startTime));

  }

  private SimpleCollector getAggregationCollector() {
    return new SimpleCollector() {
      private LeafReaderContext context;
      public int totalHits;

      public long sum;

      private StarTreeAggregatedValues obj = null;

      @Override
      public void collect(int doc)
          throws IOException {
        if(obj != null) {
          NumericDocValues dv = obj.metricValues.get("status_sum");
          dv.advanceExact(doc);
          sum += dv.longValue();
        }
        totalHits++;
      }

      @Override
      protected void doSetNextReader(LeafReaderContext context) throws IOException {

        //docsBuilder = new DocIdSetBuilder(Integer.MAX_VALUE);
        totalHits = 0;
        this.context = context;
        obj = (StarTreeAggregatedValues) context.reader().getAggregatedDocValues();
      }

      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
      }

      @Override
      public void finish() throws IOException {
        System.out.println("Star tree sum : "  + sum);
//        matchingDocs.add(new MatchingDocs(this.context, docsBuilder.build(), totalHits));
        totalHits = 0;
      }
    };

  }
}
