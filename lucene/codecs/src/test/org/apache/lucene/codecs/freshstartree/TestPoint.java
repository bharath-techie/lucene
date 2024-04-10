package org.apache.lucene.codecs.freshstartree;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.freshstartree.codec.StarTreeCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.BeforeClass;


public class TestPoint extends LuceneTestCase {
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

  public void test2dPoints2()
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
    while (docsAdded < totalDocs) {

      int hour = getRandomHour();
      int status = getRandomStatus200();

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
    queryDayRangeWithStatus(w);
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
}
