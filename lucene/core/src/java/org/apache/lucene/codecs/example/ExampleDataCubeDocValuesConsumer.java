package org.apache.lucene.codecs.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.codecs.DataCubesProducer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesConsumer;
import org.apache.lucene.index.DataCubesConfig;
import org.apache.lucene.index.DataCubeDocValuesConsumer;
import org.apache.lucene.index.DataCubeValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedNumericDocValuesWriter;
import org.apache.lucene.index.SortedSetDocValuesWriter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;


public class ExampleDataCubeDocValuesConsumer extends DataCubeDocValuesConsumer {

  Lucene90DocValuesConsumer createDocValuesConsumer;

  /**
   * expert: Creates a new writer
   *
   * @param state
   * @param dataCodec
   * @param dataExtension
   * @param metaCodec
   * @param metaExtension
   */
  public ExampleDataCubeDocValuesConsumer(SegmentWriteState state, String dataCodec, String dataExtension,
      String metaCodec, String metaExtension)
      throws IOException {
    super();
    this.createDocValuesConsumer = new Lucene90DocValuesConsumer(state, dataCodec, "sttd", metaCodec, "sttm");
  }

  @Override
  public void flush(DataCubesConfig dataCubesConfig) throws IOException {
    StarTreeConfig config = (StarTreeConfig) dataCubesConfig;
    System.out.println();
    System.out.println("======= In flush : ======");
    int fieldNum = 0;
    /* Creating dummy doc values fields in the new format */

    createDataCubeNumericFields(dataCubesConfig);

    createDocValuesConsumer.close();

  }

  private void createDataCubeFields(DataCubesConfig dataCubesConfig)
      throws IOException {
    int fieldNum = 0;
    StarTreeConfig config = (StarTreeConfig) dataCubesConfig;
    for(StarTreeField field : config.getFields()) {
      List<SortedNumericDocValuesWriter> dimWriterList = new ArrayList<>();
      List<SortedNumericDocValuesWriter> metricWriterList = new ArrayList<>();
      List<SortedSetDocValuesWriter> keywordWriterList = new ArrayList<>();
      FieldInfo[] dimFieldInfoArr = new FieldInfo[field.getDims().size()];
      FieldInfo[] metricFieldInfoArr = new FieldInfo[field.getMetrics().size()];
      int i = 0;
      for(String dim : field.getDims()) {
        final FieldInfo fi = new FieldInfo(dim + "_dim", fieldNum, false, false, true,
            IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, DocValuesType.SORTED_NUMERIC, -1, Collections.emptyMap(),
            0, 0, 0, 0, VectorEncoding.FLOAT32, VectorSimilarityFunction.EUCLIDEAN, false);
        dimFieldInfoArr[i] = fi;
        final SortedNumericDocValuesWriter w = new SortedNumericDocValuesWriter(fi, Counter.newCounter());
        dimWriterList.add(w);
        fieldNum++;
        i++;
      }
      i=0;
      for(String metric : field.getMetrics()) {

        final FieldInfo fi = new FieldInfo(metric, fieldNum, false, false, true,
            IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, DocValuesType.SORTED_NUMERIC, -1, Collections.emptyMap(),
            0, 0, 0, 0, VectorEncoding.FLOAT32, VectorSimilarityFunction.EUCLIDEAN, false);
        metricFieldInfoArr[i] = fi;
        final SortedNumericDocValuesWriter w = new SortedNumericDocValuesWriter(fi, Counter.newCounter());
        metricWriterList.add(w);
        fieldNum++;
        i++;
      }

      int dimIndex = 0;
      for(String dim : (field.getDims())) {
        SortedNumericDocValues values = DocValues.singleton(getNumericDocValuesMap().get(dim));
        int docID;
        while ((docID = values.nextDoc()) != NO_MORE_DOCS) {
          final int count = values.docValueCount();
          for (i = 0; i < count; ++i) {
            final long v = values.nextValue();
            dimWriterList.get(dimIndex).addValue(docID, v);
          }
        }
        dimIndex++;
      }

      int metricIndex = 0;
      for(String metric : field.getMetrics()){
        SortedNumericDocValues values = DocValues.singleton(getNumericDocValuesMap().get(metric));
        int docID;
        while ((docID = values.nextDoc()) != NO_MORE_DOCS) {
          final int count = values.docValueCount();
          for (i = 0; i < count; ++i) {
            final long v = values.nextValue();
            metricWriterList.get(metricIndex).addValue(docID, v);
          }
        }
        metricIndex++;
      }

      int k = 0;
      for (i = 0; i < field.getDims().size() ; i++) {
        final int finalI = k;
        DocValuesProducer producer1 = new EmptyDocValuesProducer() {
          @Override
          public SortedNumericDocValues getSortedNumeric(FieldInfo field)
              throws IOException {

            return dimWriterList.get(finalI).getDocValues();
          }
        };
        createDocValuesConsumer.addSortedNumericField(dimFieldInfoArr[i], producer1);
        k++;
      }
      k=0;
      for (i = 0; i < field.getMetrics().size() ; i++) {
        final int finalI = k;
        DocValuesProducer producer1 = new EmptyDocValuesProducer() {
          @Override
          public SortedNumericDocValues getSortedNumeric(FieldInfo field)
              throws IOException {

            return metricWriterList.get(finalI).getDocValues();
          }
        };
        createDocValuesConsumer.addSortedNumericField(metricFieldInfoArr[i], producer1);
        k++;
      }
    }
  }

  private void createDataCubeNumericFields(DataCubesConfig dataCubesConfig)
      throws IOException {
    int fieldNum = 0;
    StarTreeConfig config = (StarTreeConfig) dataCubesConfig;
    for(StarTreeField field : config.getFields()) {

      PackedLongValues.Builder[] pendingDimArr = new PackedLongValues.Builder[field.getDims().size()];
      PackedLongValues.Builder[] pendingMetricArr = new PackedLongValues.Builder[field.getMetrics().size()];

      FieldInfo[] dimFieldInfoArr = new FieldInfo[field.getDims().size()];
      FieldInfo[] metricFieldInfoArr = new FieldInfo[field.getMetrics().size()];
      int i = 0;
      for(String dim : field.getDims()) {
        pendingDimArr[i] = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
        dimFieldInfoArr[i] = new FieldInfo(dim + "_dim", fieldNum, false, false, true,
            IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, DocValuesType.NUMERIC, -1, Collections.emptyMap(),
            0, 0, 0, 0, VectorEncoding.FLOAT32, VectorSimilarityFunction.EUCLIDEAN, false);
        fieldNum++;
        i++;
      }
      i=0;
      for(String metric : field.getMetrics()) {
        pendingMetricArr[i] = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
        final FieldInfo fi = new FieldInfo(metric, fieldNum, false, false, true,
            IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, DocValuesType.NUMERIC, -1, Collections.emptyMap(),
            0, 0, 0, 0, VectorEncoding.FLOAT32, VectorSimilarityFunction.EUCLIDEAN, false);
        metricFieldInfoArr[i] = fi;
        fieldNum++;
        i++;
      }
      DocsWithFieldSet docsWithField = new DocsWithFieldSet();
      int dimIndex = 0;
      for(String dim : (field.getDims())) {
        SortedNumericDocValues values = DocValues.singleton(getNumericDocValuesMap().get(dim));
        int docID;
        while ((docID = values.nextDoc()) != NO_MORE_DOCS) {
          if(dimIndex == 0)
          docsWithField.add(docID);
          final int count = values.docValueCount();
          for (int l = 0; l < count; ++l) {
            final long v = values.nextValue();
            pendingDimArr[dimIndex].add(v);
          }
        }
        dimIndex++;
      }

      int metricIndex = 0;
      for(String metric : field.getMetrics()){
        SortedNumericDocValues values = DocValues.singleton(getNumericDocValuesMap().get(metric));
        int docID;
        while ((docID = values.nextDoc()) != NO_MORE_DOCS) {
          final int count = values.docValueCount();
          for (int l = 0; l < count; ++l) {
            final long v = values.nextValue();
            System.out.println("Metric : " + v*2);
            pendingMetricArr[metricIndex].add(v*2);
          }
        }
        metricIndex++;
      }

      int k = 0;
      for (i = 0; i < field.getDims().size() ; i++) {
        final int finalI = k;
        DocValuesProducer producer1 = new EmptyDocValuesProducer() {
          @Override
          public NumericDocValues getNumeric(FieldInfo field) throws IOException {

            return new BufferedAggregatedDocValues(pendingDimArr[finalI].build(), docsWithField.iterator());
          }
        };
        System.out.println("Here : adding numeric dim field = " + i);
        createDocValuesConsumer.addNumericField(dimFieldInfoArr[i], producer1);
        k++;
      }
      k=0;
      for (i = 0; i < field.getMetrics().size() ; i++) {
        final int finalI = k;
        DocValuesProducer producer1 = new EmptyDocValuesProducer() {
          @Override
          public NumericDocValues getNumeric(FieldInfo field) throws IOException {

            return new BufferedAggregatedDocValues(pendingMetricArr[finalI].build(), docsWithField.iterator());
          }
        };
        System.out.println("Here : adding numeric metric field = " + i);
        createDocValuesConsumer.addNumericField(metricFieldInfoArr[i], producer1);
        k++;
      }
    }
  }

  @Override
  public void merge(MergeState mergeState) throws IOException {
    super.merge(mergeState);
    List<StarTreeAggregatedValues> aggrList = new ArrayList<>();
    List<String> dimNames = new ArrayList<>();
    for (int i = 0; i < mergeState.dataCubesReaders.length; i++) {
      DataCubesProducer<?> producer = mergeState.dataCubesReaders[i];
      @SuppressWarnings("unchecked")
      DataCubeValues<StarTreeAggregatedValues> starTree =
          (DataCubeValues<StarTreeAggregatedValues>) producer.getDataCubeValues("field_place_holder");
    }
    long startTime = System.currentTimeMillis();
    // fix this
    // TODO : han
  }


}
