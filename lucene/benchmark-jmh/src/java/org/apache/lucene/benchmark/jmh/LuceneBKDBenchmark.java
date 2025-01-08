package org.apache.lucene.benchmark.jmh;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class LuceneBKDBenchmark {
    private static final String FIELD = "field";
    private static final Random RANDOM = new Random(578136438746182L);

    @Param({"32", "128", "1024", "8192", "1048576"})
    private int cardinality;

    @Param({"100000000"})
    private int docCount;

    private Directory directory;
    private IndexReader indexReader;
    private IndexSearcher searcher;
    private static File indexDir;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        // Create index directory
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        indexDir = new File(tmpDir, "lucene-bkd-benchmark-" + cardinality + "-" + docCount);
        if (!indexDir.exists()) {
            createIndex();
        }

        // Open index
        directory = FSDirectory.open(indexDir.toPath());
        indexReader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(indexReader);
        searcher.setQueryCachingPolicy(new QueryCachingPolicy() {
            @Override
            public void onUse(Query query) {}

            @Override
            public boolean shouldCache(Query query) {
                return false;
            }
        });
    }

    private void createIndex() throws IOException {
        Directory dir = FSDirectory.open(indexDir.toPath());
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setUseCompoundFile(false);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setCodec(new Lucene101Codec());

        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (int i = 0; i < docCount; i++) {
                Document document = new Document();
                long point = RANDOM.nextInt(cardinality);
                document.add(new LongPoint(FIELD, point));
                writer.addDocument(document);
            }
            writer.flush();
            writer.forceMerge(1);
        }
        dir.close();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        indexReader.close();
        directory.close();
    }

    @Benchmark
    @OperationsPerInvocation(1)
    public void pointQuery1Term() throws IOException {
        Query query = getPointQuery(1);
        doSearch(searcher, query);
    }

    @Benchmark
    @OperationsPerInvocation(1)
    public void pointQuery8Terms() throws IOException {
        Query query = getPointQuery(8);
        doSearch(searcher, query);
    }

    @Benchmark
    @OperationsPerInvocation(1)
    public void pointQuery16Terms() throws IOException {
        Query query = getPointQuery(16);
        doSearch(searcher, query);
    }

    @Benchmark
    @OperationsPerInvocation(1)
    public void rangeQuery1Percent() throws IOException {
        Query query = getRangeQuery(0.01);
        doSearch(searcher, query);
    }

    @Benchmark
    @OperationsPerInvocation(1)
    public void rangeQuery10Percent() throws IOException {
        Query query = getRangeQuery(0.1);
        doSearch(searcher, query);
    }

    @Benchmark
    @OperationsPerInvocation(1)
    public void rangeQuery30Percent() throws IOException {
        Query query = getRangeQuery(0.3);
        doSearch(searcher, query);
    }

    private Query getPointQuery(int termCount) {
        Set<Integer> set = new HashSet<>(termCount);
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

    private Query getRangeQuery(double selectivity) {
        long min = RANDOM.nextInt(cardinality);
        long range = (long) (cardinality * selectivity);
        long max = Math.min(min + range, cardinality - 1);
        return LongPoint.newRangeQuery(FIELD, min, max);
    }

    private static void doSearch(IndexSearcher searcher, Query query) throws IOException {
        searcher.search(query, new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) {
                return new LeafCollector() {
                    @Override
                    public void setScorer(Scorable scorer) {}

                    @Override
                    public void collect(int doc) {
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

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LuceneBKDBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}