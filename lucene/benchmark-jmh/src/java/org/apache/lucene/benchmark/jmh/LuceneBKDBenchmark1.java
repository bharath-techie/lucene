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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class LuceneBKDBenchmark1 {
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

    // Stats storage
    private Map<String, Long> indexStats;

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

        // Collect index statistics
        collectIndexStats();
    }

    private void collectIndexStats() throws IOException {
        indexStats = new HashMap<>();

        // Total index size
        long totalSize = Files.walk(indexDir.toPath())
                .filter(Files::isRegularFile)
                .mapToLong(p -> p.toFile().length())
                .sum();
        indexStats.put("totalIndexSize", totalSize);

        // File-specific sizes
        Files.walk(indexDir.toPath())
                .filter(Files::isRegularFile)
                .forEach(p -> {
                    try {
                        indexStats.put("file_" + p.getFileName(), Files.size(p));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

        // Segment information
        for (LeafReaderContext context : indexReader.leaves()) {
            SegmentReader segReader = (SegmentReader) context.reader();
            SegmentInfo segInfo = segReader.getSegmentInfo().info;
            indexStats.put("segment_" + segInfo.name + "_docCount", (long) segInfo.maxDoc());
            indexStats.put("segment_" + segInfo.name + "_size", segReader.getSegmentInfo().sizeInBytes());
        }

        // Log the statistics
        System.out.println("\n=== Index Statistics for Cardinality " + cardinality + " ===");
        System.out.println("Total Index Size: " + formatSize(indexStats.get("totalIndexSize")));
        System.out.println("\nFile Sizes:");
        indexStats.entrySet().stream()
                .filter(e -> e.getKey().startsWith("file_"))
                .forEach(e -> System.out.println(e.getKey() + ": " + formatSize(e.getValue())));
        System.out.println("\nSegment Information:");
        indexStats.entrySet().stream()
                .filter(e -> e.getKey().startsWith("segment_"))
                .forEach(e -> System.out.println(e.getKey() + ": " +
                        (e.getKey().endsWith("_size") ? formatSize(e.getValue()) : e.getValue())));
    }

    private String formatSize(long bytes) {
        String[] units = {"B", "KB", "MB", "GB", "TB"};
        int unitIndex = 0;
        double size = bytes;
        while (size >= 1024 && unitIndex < units.length - 1) {
            size /= 1024;
            unitIndex++;
        }
        return String.format("%.2f %s", size, units[unitIndex]);
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

                // Log progress every 10M documents
                if (i > 0 && i % 10_000_000 == 0) {
                    System.out.println("Indexed " + i + " documents...");
                }
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

    // Benchmarks with result tracking
    @Benchmark
    public void pointQuery1Term(Blackhole blackhole) throws IOException {
        Query query = getPointQuery(1);
        long hitCount = doSearch(searcher, query);
        blackhole.consume(hitCount);
    }

    @Benchmark
    public void pointQuery8Terms(Blackhole blackhole) throws IOException {
        Query query = getPointQuery(8);
        long hitCount = doSearch(searcher, query);
        blackhole.consume(hitCount);
    }

    @Benchmark
    public void pointQuery16Terms(Blackhole blackhole) throws IOException {
        Query query = getPointQuery(16);
        long hitCount = doSearch(searcher, query);
        blackhole.consume(hitCount);
    }

    @Benchmark
    public void rangeQuery1Percent(Blackhole blackhole) throws IOException {
        Query query = getRangeQuery(0.01);
        long hitCount = doSearch(searcher, query);
        blackhole.consume(hitCount);
    }

    @Benchmark
    public void rangeQuery10Percent(Blackhole blackhole) throws IOException {
        Query query = getRangeQuery(0.1);
        long hitCount = doSearch(searcher, query);
        blackhole.consume(hitCount);
    }

    @Benchmark
    public void rangeQuery30Percent(Blackhole blackhole) throws IOException {
        Query query = getRangeQuery(0.3);
        long hitCount = doSearch(searcher, query);
        blackhole.consume(hitCount);
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

    private static long doSearch(IndexSearcher searcher, Query query) throws IOException {
        final long[] hitCount = {0};
        searcher.search(query, new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) {
                return new LeafCollector() {
                    @Override
                    public void setScorer(Scorable scorer) {}

                    @Override
                    public void collect(int doc) {
                        hitCount[0]++;
                    }
                };
            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }
        });
        return hitCount[0];
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LuceneBKDBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.JSON)
                .result("benchmark-results.json")
                .build();

        new Runner(opt).run();
    }

    // Helper method to get index statistics
    public Map<String, Long> getIndexStats() {
        return Collections.unmodifiableMap(indexStats);
    }
}