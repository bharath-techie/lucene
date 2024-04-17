package org.apache.lucene.codecs.example;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.CompositeValuesReader;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesProducer;
import org.apache.lucene.index.CompositeIndexField;
import org.apache.lucene.index.CompositeValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.IndexInput;


public class ExampleCompositeValuesReader extends CompositeValuesReader<StarTreeAggregatedValues> {

  public static final String DATA_CODEC = "Lucene90DocValuesData";
  public static final String META_CODEC = "Lucene90DocValuesMetadata";
  DocValuesProducer valuesProducer;
  private IndexInput data;
  Map<String, SortedNumericDocValues> dimensionValues;
  Map<String, SortedNumericDocValues> metricValues;
  Map<String, SortedSetDocValues> keywordDimValues;
  StarTree starTree= null;
  public ExampleCompositeValuesReader(SegmentReadState state)
      throws IOException {
    //String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "stttree");
   // this.data = state.directory.openInput(dataName, state.context);

    System.out.println("======== Reading star tree from segment : " + state.segmentInfo.name);
    //System.out.println(state.segmentInfo.getCompositeConfig());
    for(CompositeIndexField compositeIndexField : state.segmentInfo.getCompositeConfig().getFields()) {
      StarTreeCompositeIndexField sc = (StarTreeCompositeIndexField ) compositeIndexField;
      System.out.println("Composite field name : " + sc.getName());
      System.out.println("Fields : " + Arrays.toString(sc.getFields().toArray()));
      System.out.println("Dims : " + Arrays.toString(sc.getDims().toArray()));
      System.out.println("Metrics : " + Arrays.toString(sc.getMetrics().toArray()));

    }

    // TODO : fix this -> how to get rid of last param
    //valuesProducer = new Lucene90DocValuesProducer(state, DATA_CODEC, "sttd", META_CODEC, "sttm", starTree.getDimensionNames());
    dimensionValues = new LinkedHashMap<>();
    keywordDimValues = new LinkedHashMap<>();
    metricValues = new LinkedHashMap<>();
  }

  @Override
  public void checkIntegrity()
      throws IOException {

  }

  @Override
  public CompositeValues<StarTreeAggregatedValues> getCompositeFieldValues(String field)
      throws IOException {
    return null;
  }

  public StarTreeAggregatedValues getAggregatedDocValues() throws IOException {
    List<String> dimensionsSplitOrder = starTree.getDimensionNames();
    for (int i = 0; i < dimensionsSplitOrder.size(); i++) {
      try {
        //dimensionValues.put(dimensionsSplitOrder.get(i), valuesProducer.getSortedNumeric(dimensionsSplitOrder.get(i) + "_dim"));
      } catch (NullPointerException e) {
       // keywordDimValues.put(dimensionsSplitOrder.get(i), valuesProducer.getSortedSet(dimensionsSplitOrder.get(i) + "_dim"));
      }
    }
    metricValues = new HashMap<>();
    // TODO : give field info
   // metricValues.put("status_sum", valuesProducer.getSortedNumeric("status_sum_metric"));
    //metricValues.put("status_count", valuesProducer.getNumeric("status_count_metric"));
    return new StarTreeAggregatedValues(starTree, dimensionValues, keywordDimValues, metricValues);
  }

  @Override
  public void close()
      throws IOException {

  }
}
