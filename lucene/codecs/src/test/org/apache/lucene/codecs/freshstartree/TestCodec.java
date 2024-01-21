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
package org.apache.lucene.codecs.freshstartree;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.freshstartree.codec.StarTreeAggregatedValues;
import org.apache.lucene.codecs.freshstartree.codec.StarTreeCodec;
import org.apache.lucene.codecs.freshstartree.query.StarTreeQuery;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NumericUtils;
import org.junit.Test;

import org.junit.BeforeClass;


@LuceneTestCase.SuppressSysoutChecks(bugUrl="stuff gets printed")
public class TestCodec extends LuceneTestCase {
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

  private static int getRandomHour() {
    int minHour = 0;
    int maxHour = 23;
    Random random = new Random();
    return random.nextInt(maxHour - minHour + 1) + minHour;
  }

  private static int getRandomInt() {
    int minHour = 0;
    int maxHour = 100000;
    Random random = new Random();
    return random.nextInt(maxHour - minHour + 1) + minHour;
  }

  @Test
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

    int totalDocs = 1000000;
    int docsAdded = 0;

    while (docsAdded < totalDocs) {

      int hour = getRandomHour();
      int day = getRandomDay();
      int status = getRandomStatus();

      for (int i = 0; i < 100; i++) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("hour", hour));
        doc.add(new SortedNumericDocValuesField("day", day));
        doc.add(new SortedNumericDocValuesField("status", status));
        w.addDocument(doc);
      }

      docsAdded += 100;
    }

    w.flush();
    query(w);
    totalDocs = 1000000;
    docsAdded = 0;
    while (docsAdded < totalDocs) {

      int hour = getRandomHour();
      int day = getRandomDay();
      int status = getRandomStatus();

      for (int i = 0; i < 100; i++) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("hour", hour));
        doc.add(new SortedNumericDocValuesField("day", day));
        doc.add(new SortedNumericDocValuesField("status", status));
        w.addDocument(doc);
      }

      docsAdded += 100;
    }
    w.flush();
    query(w);
    w.forceMerge(1);
    query(w);
  }

  public void testStarTreeHighCardBelowMaxDoc()
      throws IOException {
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setUseCompoundFile(false);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    config.setCodec(new StarTreeCodec());
    Directory luceneDir = FSDirectory.open(plaintextDir.toPath());
    System.out.println("Dir : " + plaintextDir.toPath());
    IndexWriter w = new IndexWriter(luceneDir, config);

    int totalDocs = 1000000;
    int docsAdded = 0;

    while (docsAdded < totalDocs) {

      int hour = getRandomInt();
      int day = getRandomInt();
      int status = 1;//getRandomInt();

      for(int i=0; i<10; i++) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("hour", hour));
        doc.add(new SortedNumericDocValuesField("day", day));
        doc.add(new SortedNumericDocValuesField("status", status));
        w.addDocument(doc);
      }

      docsAdded += 10;
    }

    w.flush();
    queryWithGroupBy(w);
    totalDocs = 1000000;
    docsAdded = 0;
    while (docsAdded < totalDocs) {

      int hour = getRandomInt();
      int day = getRandomInt();
      int status = 1;//getRandomInt();

      for(int i=0; i<10; i++) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("hour", hour));
        doc.add(new SortedNumericDocValuesField("day", day));
        doc.add(new SortedNumericDocValuesField("status", status));
        w.addDocument(doc);
      }

      docsAdded += 10;
    }
    w.flush();
    queryWithGroupBy(w);
    w.forceMerge(1);
    queryWithGroupBy(w);
  }

  public void testStarTreeFilter()
      throws IOException {
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setUseCompoundFile(false);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    config.setCodec(new StarTreeCodec());
    Directory luceneDir = FSDirectory.open(plaintextDir.toPath());
    System.out.println("Dir : " + plaintextDir.toPath());
    IndexWriter w = new IndexWriter(luceneDir, config);


    int totalDocs = 1000000;
    int docsAdded = 0;

    int count = 0;
    while (docsAdded < totalDocs) {

      int hour = getRandomHour();
      int day = getRandomDay();
      int status = getRandomStatus();//1;//getRandomInt();

      for(int i=0; i<10; i++) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("hour", hour));
        doc.add(new SortedNumericDocValuesField("day", day));
        doc.add(new SortedNumericDocValuesField("status", status));
        if(status == 200 && (day == 30 || (day > 2 && day < 5))) {
          count++;
        }
        w.addDocument(doc);
      }

      docsAdded += 10;
    }

    long startTime = System.currentTimeMillis();
    w.flush();
    System.out.println("Overall flush time : " + (System.currentTimeMillis() - startTime));
    System.out.println("========= Expected sum ========== " + (count * 200));
    queryWithFilter(w);
    totalDocs = 1000000;
    docsAdded = 0;
    //count = 0;
    while (docsAdded < totalDocs) {

      int hour = getRandomHour();
      int day = getRandomDay();
      int status = getRandomStatus();//1;//getRandomInt();

      for(int i=0; i<10; i++) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("hour", hour));
        doc.add(new SortedNumericDocValuesField("day", day));
        doc.add(new SortedNumericDocValuesField("status", status));
        if(status == 200 && (day == 30 || (day > 2 && day < 5))) {
          count++;
        }
        w.addDocument(doc);
      }

      docsAdded += 10;
    }

    startTime = System.currentTimeMillis();
    w.flush();
    System.out.println("Overall flush time : " + (System.currentTimeMillis() - startTime));
    System.out.println("===== Expected sum  ===========" + (count * 200));
    queryWithFilter(w);
    startTime = System.currentTimeMillis();
    w.forceMerge(1);
    System.out.println("Overall merge time : " + (System.currentTimeMillis() - startTime));
    queryWithFilter(w);
  }

  public void testStarTreeGroupBy()
      throws IOException {
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setUseCompoundFile(false);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    config.setCodec(new StarTreeCodec());
    Directory luceneDir = FSDirectory.open(plaintextDir.toPath());
    System.out.println("Dir : " + plaintextDir.toPath());
    IndexWriter w = new IndexWriter(luceneDir, config);

    int totalDocs = 1000000;
    int docsAdded = 0;

    int count = 0;
    Map<Integer, Integer> statusToCountMap = new HashMap<>();
    while (docsAdded < totalDocs) {

      int hour = getRandomHour();
      int day = getRandomDay();
      int status = getRandomStatus();//1;//getRandomInt();

      for(int i=0; i<10; i++) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("hour", hour));
        doc.add(new SortedNumericDocValuesField("day", day));
        doc.add(new SortedNumericDocValuesField("status", status));
        statusToCountMap.put(status, (statusToCountMap.getOrDefault(status, 0) + 1 ));
        w.addDocument(doc);
      }

      docsAdded += 10;
    }

    w.flush();
    //System.out.println("========= Expected sum ========== " + (count * 200));
    queryWithGroupBy(w);
    totalDocs = 1000000;
    docsAdded = 0;
    //count = 0;
    while (docsAdded < totalDocs) {

      int hour = getRandomHour();
      int day = getRandomDay();
      int status = getRandomStatus();//1;//getRandomInt();

      for(int i=0; i<10; i++) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("hour", hour));
        doc.add(new SortedNumericDocValuesField("day", day));
        doc.add(new SortedNumericDocValuesField("status", status));
        statusToCountMap.put(status, (statusToCountMap.getOrDefault(status, 0) + 1 ));
        w.addDocument(doc);
      }

      docsAdded += 10;
    }

    System.out.println(" =================================== ");
    for(Map.Entry<Integer, Integer> entry : statusToCountMap.entrySet()) {
      System.out.println("Expected Status = " + entry.getKey()  + " Expected Sum = " +
      entry.getValue() * entry.getKey() );
    }
    System.out.println(" =================================== ");

    w.flush();
    System.out.println("===== Expected sum  ===========" + (count * 200));
    queryWithGroupBy(w);
    w.forceMerge(1);
    queryWithGroupBy(w);
  }

  public void testJustQueryGroupByOffHeap()
      throws IOException {
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setUseCompoundFile(false);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    config.setCodec(new StarTreeCodec());
    Directory luceneDir = FSDirectory.open(plaintextDir.toPath());
    System.out.println("Dir : " + plaintextDir.toPath());
    IndexWriter w = new IndexWriter(luceneDir, config);
    queryWithGroupBy(w);
  }

  public void testStarTreeGroupByOffHeap()
      throws IOException {
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setUseCompoundFile(false);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    config.setCodec(new StarTreeCodec());
    Directory luceneDir = FSDirectory.open(plaintextDir.toPath());
    System.out.println("Dir : " + plaintextDir.toPath());
    IndexWriter w = new IndexWriter(luceneDir, config);
    for(int i=0; i<1; i++) {

      int totalDocs = 10000000;
      int docsAdded = 0;

      //int count = 0;
      Map<Integer, Integer> statusToCountMap = new HashMap<>();
      while (docsAdded < totalDocs) {

        int hour = getRandomHour();
        int day = getRandomDay();
        int status = getRandomStatus();//1;//getRandomInt();

        for (int k = 0; k < 10; k++) {
          Document doc = new Document();
          doc.add(new SortedNumericDocValuesField("hour", hour));
          doc.add(new SortedNumericDocValuesField("day", day));
          doc.add(new SortedNumericDocValuesField("status", status));
          statusToCountMap.put(status, (statusToCountMap.getOrDefault(status, 0) + 1));
          w.addDocument(doc);
        }

        docsAdded += 10;
      }
      for (Map.Entry<Integer, Integer> entry : statusToCountMap.entrySet()) {
        System.out.println("Expected Status = " + entry.getKey() + " Expected Sum = " + entry.getValue() * entry.getKey());
      }
      long startTime = System.currentTimeMillis();
      w.flush();
      System.out.println("Flush took : " + (System.currentTimeMillis() - startTime));

      queryWithGroupBy(w);
    }
    long startTime = System.currentTimeMillis();
    w.forceMerge(1);
    System.out.println("Merge took : " + (System.currentTimeMillis() - startTime));
    System.out.println("=== QUERYING after merge ====");
    queryWithGroupBy(w);
  }

  public void testNonStarTreeGroupByOffHeap()
      throws IOException {
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setUseCompoundFile(false);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    Directory luceneDir = FSDirectory.open(plaintextDir.toPath());
    System.out.println("Dir : " + plaintextDir.toPath());
    IndexWriter w = new IndexWriter(luceneDir, config);

    int totalDocs = 100000;
    int docsAdded = 0;

    //int count = 0;
    Map<Integer, Integer> statusToCountMap = new HashMap<>();
    while (docsAdded < totalDocs) {

      int hour = getRandomHour();
      int day = getRandomDay();
      int status = getRandomStatus();

      for(int i=0; i<10; i++) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("hour", hour));
        doc.add(new SortedNumericDocValuesField("day", day));
        doc.add(new SortedNumericDocValuesField("status", status));
        statusToCountMap.put(status, (statusToCountMap.getOrDefault(status, 0) + 1 ));
        w.addDocument(doc);
      }

      docsAdded += 10;
    }
    long startTime = System.currentTimeMillis();
    w.flush();
    System.out.println("Flush took : " + (System.currentTimeMillis() - startTime));
    final IndexReader reader = DirectoryReader.open(w);
    final IndexSearcher searcher = newSearcher(reader, false);
    startTime = System.currentTimeMillis();
    final Query q1 = new MatchAllDocsQuery();
    searcher.search(q1, getSumCollector());
    System.out.println("Finished querying normal doc values in ms : " +
        (System.currentTimeMillis() - startTime));
  }

  public void testStarTreeHighCard() // This breaks the flows - since no of docs are > max docs in lucene
      throws Exception {
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setUseCompoundFile(false);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    config.setCodec(new StarTreeCodec());
    Directory luceneDir = FSDirectory.open(plaintextDir.toPath());
    System.out.println("Dir : " + plaintextDir.toPath());
    IndexWriter w = new IndexWriter(luceneDir, config);

    int totalDocs = 1000000;
    int docsAdded = 0;

    while (docsAdded < totalDocs) {

      int hour = getRandomInt();
      int day = getRandomInt();
      int status = getRandomInt();

      Document doc = new Document();
      doc.add(new SortedNumericDocValuesField("hour", hour));
      doc.add(new SortedNumericDocValuesField("day", day));
      doc.add(new SortedNumericDocValuesField("status", status));
      w.addDocument(doc);

      docsAdded++;// += 100;
    }

    w.flush();
    queryWithGroupBy(w);
    totalDocs = 1000000;
    docsAdded = 0;
    while (docsAdded < totalDocs) {

      int hour = getRandomInt();
      int day = getRandomInt();
      int status = getRandomInt();

      Document doc = new Document();
      doc.add(new SortedNumericDocValuesField("hour", hour));
      doc.add(new SortedNumericDocValuesField("day", day));
      doc.add(new SortedNumericDocValuesField("status", status));
      w.addDocument(doc);

      docsAdded++;// += 100;
    }
    w.flush();
    queryWithGroupBy(w);
    w.forceMerge(1);
    queryWithGroupBy(w);

    //System.out.println(td2.totalHits.value);
    //System.out.println("done : " + luceneDir.toString());
  }

  private void query(IndexWriter w)
      throws IOException {
    long startTime = System.currentTimeMillis();

    final IndexReader reader = DirectoryReader.open(w);
    final IndexSearcher searcher = newSearcher(reader, false);
    final Query q = new StarTreeQuery(new HashMap<>(), new HashSet<>());
//    StarTreeAggregatedValues a = (StarTreeAggregatedValues)
    reader.leaves().get(0).reader().getAggregatedDocValues();
//    a._starTree.printTree(new HashMap<>());
    searcher.search(q, getAggregationCollector());
    System.out.println("============== Finished querying star-tree in ms : " +
        (System.currentTimeMillis() - startTime));

    startTime = System.currentTimeMillis();
    final Query q1 = new MatchAllDocsQuery();
    searcher.search(q1, getSumCollector());
    System.out.println("Finished querying normal doc values in ms : " +
        (System.currentTimeMillis() - startTime));

  }

  private void queryWithGroupBy(IndexWriter w)
      throws IOException {
    long startTime = System.currentTimeMillis();

    final IndexReader reader = DirectoryReader.open(w);
    final IndexSearcher searcher = newSearcher(reader, false);
    Set<String> groupByCols = new HashSet<>();
    groupByCols.add("day");
    groupByCols.add("status");
    Map<String, List<Predicate<Integer>>> predicateMap = new HashMap<>();
    List<Predicate<Integer>> predicates = new ArrayList<>();
    predicates.add(status -> status == 200);
    predicateMap.put("status", predicates);
    final Query q = new StarTreeQuery(new HashMap<>(), groupByCols);

    searcher.search(q, getGroupByAggregationCollector());
    System.out.println("============== Finished querying star-tree in ms : " +
        (System.currentTimeMillis() - startTime));

    startTime = System.currentTimeMillis();
    final Query q1 = new MatchAllDocsQuery();
    searcher.search(q1, getSumCollector());
    System.out.println("Finished querying normal doc values in ms : " +
        (System.currentTimeMillis() - startTime));
  }

  private void queryWithFilter(IndexWriter w)
      throws IOException {
    long startTime = System.currentTimeMillis();

    final IndexReader reader = DirectoryReader.open(w);
    final IndexSearcher searcher = newSearcher(reader, false);
    Set<String> groupByCols = new HashSet<>();
    //groupByCols.add("day");
    Map<String, List<Predicate<Integer>>> predicateMap = new HashMap<>();
    List<Predicate<Integer>> predicates = new ArrayList<>();
    predicates.add(day -> day > 2 && day < 5);
    predicates.add(day -> day == 30);
    predicateMap.put("day", predicates);
    predicates = new ArrayList<>();
    predicates.add(status -> status == 200);
    predicateMap.put("status", predicates);
    final Query q = new StarTreeQuery(predicateMap, groupByCols);

    searcher.search(q, getAggregationCollector());
    System.out.println("=============== Finished querying star-tree in ms : " +
        (System.currentTimeMillis() - startTime));

    startTime = System.currentTimeMillis();
    final Query q1 = new MatchAllDocsQuery();
    searcher.search(q1, getSumCollector());
    System.out.println("Finished querying normal doc values in ms : " +
        (System.currentTimeMillis() - startTime));

  }

  private SimpleCollector getSumCollector() {
    return new SimpleCollector() {
      private LeafReaderContext context;
      private DocIdSetBuilder docsBuilder;
      public int totalHits;
      private final List<MatchingDocs> matchingDocs = new ArrayList<>();

      public long sum = 0;
      final class MatchingDocs {

        /** Context for this segment. */
        public final LeafReaderContext context;

        /** Which documents were seen. */
        public final DocIdSet bits;

        /** Total number of hits */
        public final int totalHits;

        /** Sole constructor. */
        public MatchingDocs(LeafReaderContext context, DocIdSet bits, int totalHits) {
          this.context = context;
          this.bits = bits;
          this.totalHits = totalHits;
        }
      }
      @Override
      public void collect(int doc)
          throws IOException {
        docsBuilder.grow(1).add(doc);
        SortedNumericDocValues dv = context.reader().getSortedNumericDocValues("status");
        dv.advanceExact(doc);
        sum += dv.nextValue();
        totalHits++;
      }

      @Override
      protected void doSetNextReader(LeafReaderContext context) throws IOException {
        assert docsBuilder == null;
        docsBuilder = new DocIdSetBuilder(Integer.MAX_VALUE);
        totalHits = 0;
        this.context = context;
      }

      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
      }

      @Override
      public void finish() throws IOException {
        //System.out.println("SUM in normal query : " + sum);
        matchingDocs.add(new MatchingDocs(this.context, docsBuilder.build(), totalHits));
        docsBuilder = null;
        context = null;
      }
    };
  }
  private SimpleCollector getAggregationCollector() {
    return new SimpleCollector() {
      private LeafReaderContext context;
      private DocIdSetBuilder docsBuilder;
      public int totalHits;

      public int sum;
      private final List<MatchingDocs> matchingDocs = new ArrayList<>();


      final class MatchingDocs {

        /** Context for this segment. */
        public final LeafReaderContext context;

        /** Which documents were seen. */
        public final DocIdSet bits;

        /** Total number of hits */
        public final int totalHits;

        /** Sole constructor. */
        public MatchingDocs(LeafReaderContext context, DocIdSet bits, int totalHits) {
          this.context = context;
          this.bits = bits;
          this.totalHits = totalHits;
        }
      }
      @Override
      public void collect(int doc)
          throws IOException {
        docsBuilder.grow(1).add(doc);
        //context.reader().get
        Object obj = context.reader().getAggregatedDocValues();
        if(obj != null) {
          StarTreeAggregatedValues val = (StarTreeAggregatedValues) obj;
          NumericDocValues dv = val.metricValues.get("status_sum");
          NumericDocValues dv1 = val.dimensionValues.get("day");
          NumericDocValues dv2 = val.dimensionValues.get("status");
          NumericDocValues dv3 = val.dimensionValues.get("hour");
          dv.advanceExact(doc);
          dv1.advanceExact(doc);
          dv2.advanceExact(doc);
          dv3.advance(doc);
//          System.out.println("ID = " + dv.docID() + " Day Value = " + dv1.longValue() + " Hour
        //  val = " + dv3.longValue()
//              + " Status = " + dv2.longValue() + " Sum =" + dv.longValue());

          sum += dv.longValue();
        }
        totalHits++;
      }

      @Override
      protected void doSetNextReader(LeafReaderContext context) throws IOException {
        assert docsBuilder == null;

        docsBuilder = new DocIdSetBuilder(Integer.MAX_VALUE);
        totalHits = 0;
        this.context = context;
      }

      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
      }

      @Override
      public void finish() throws IOException {
        System.out.println("Star tree sum : "  + sum);
        matchingDocs.add(new MatchingDocs(this.context, docsBuilder.build(), totalHits));
        totalHits = 0;
        docsBuilder = null;
        context = null;
      }
    };

  }

  private SimpleCollector getGroupByAggregationCollector() {
    return new SimpleCollector() {
      private LeafReaderContext context;
      private DocIdSetBuilder docsBuilder;
      public int totalHits;

      public int sum;
      private final List<MatchingDocs> matchingDocs = new ArrayList<>();

      Map<Long, Long> statusToSumMap = new HashMap<>();


      final class MatchingDocs {

        /** Context for this segment. */
        public final LeafReaderContext context;

        /** Which documents were seen. */
        public final DocIdSet bits;

        /** Total number of hits */
        public final int totalHits;

        /** Sole constructor. */
        public MatchingDocs(LeafReaderContext context, DocIdSet bits, int totalHits) {
          this.context = context;
          this.bits = bits;
          this.totalHits = totalHits;
        }
      }
      @Override
      public void collect(int doc)
          throws IOException {
        docsBuilder.grow(1).add(doc);
        Object obj = context.reader().getAggregatedDocValues();
        if(obj != null) {
          StarTreeAggregatedValues val = (StarTreeAggregatedValues) obj;
          NumericDocValues dv = val.metricValues.get("status_sum");
          NumericDocValues dv1 = val.dimensionValues.get("day");
          NumericDocValues dv2 = val.dimensionValues.get("status");
          NumericDocValues dv3 = val.dimensionValues.get("hour");
          dv.advanceExact(doc);
          dv1.advanceExact(doc);
          dv2.advanceExact(doc);
          dv3.advance(doc);
          long statusval = dv2.longValue();
          long sumval = dv.longValue();
//          System.out.println("ID = " + dv.docID() + " Day Value = " + dv1.longValue() + " Hour val = " + dv3.longValue()
//              + " Status = " + statusval + " Sum =" + sumval);
          statusToSumMap.put(statusval, statusToSumMap.getOrDefault(statusval, 0l) +
              sumval);
          sum += sumval;
        }
        totalHits++;
      }

      @Override
      protected void doSetNextReader(LeafReaderContext context) throws IOException {
        assert docsBuilder == null;

        docsBuilder = new DocIdSetBuilder(Integer.MAX_VALUE);
        totalHits = 0;
        this.context = context;
      }

      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
      }

      @Override
      public void finish() throws IOException {
        System.out.println("Star tree group by sum : "  + sum);
        for(Map.Entry<Long,Long> entry : statusToSumMap.entrySet()) {
          System.out.println("Status = " + entry.getKey() + " Sum = " + entry.getValue());
        }
        matchingDocs.add(new MatchingDocs(this.context, docsBuilder.build(), totalHits));
        totalHits = 0;
        docsBuilder = null;
        context = null;
      }
    };

  }
}
