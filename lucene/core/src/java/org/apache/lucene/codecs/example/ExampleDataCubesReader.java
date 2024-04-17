package org.apache.lucene.codecs.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.DataCubesProducer;
import org.apache.lucene.index.DataCubeField;
import org.apache.lucene.index.DataCubeValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.IndexInput;


public class ExampleDataCubesReader extends DataCubesProducer<StarTreeAggregatedValues> {

  public static final String DATA_CODEC = "Lucene90DocValuesData";
  public static final String META_CODEC = "Lucene90DocValuesMetadata";
  StarTreeCustomDocValuesProducer valuesProducer;
  private IndexInput data;
  Map<String, StarTreeAggregatedValues> starTreeAggregatedValuesMap = new HashMap<>();
  Map<String, NumericDocValues> dimensionValues = new HashMap<>();
  Map<String, NumericDocValues> metricValues = new HashMap<>();
  StarTree starTree= null;
  public ExampleDataCubesReader(SegmentReadState state)
      throws IOException {
    //String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "stttree");
   // this.data = state.directory.openInput(dataName, state.context);

    System.out.println("======== Reading star tree from segment : " + state.segmentInfo.name);
    valuesProducer = new StarTreeCustomDocValuesProducer(state, DATA_CODEC, "sttd", META_CODEC, "sttm", state.segmentInfo.getDataCubesConfig());
    for(DataCubeField compositeIndexField : state.segmentInfo.getDataCubesConfig().getFields()) {
      StarTreeField sc = (StarTreeField) compositeIndexField;

      for(String dim : sc.getDims()) {
        NumericDocValues ndv = valuesProducer.getNumeric(dim);
        if(ndv != null) {
          System.out.println("dim");
          dimensionValues.put(dim, ndv);
        }
      }
      for(String metric : sc.getMetrics()) {
        NumericDocValues ndv = valuesProducer.getNumeric(metric);
        if(ndv != null) {
          System.out.println("metric");
          metricValues.put(metric, ndv);
        }
      }

      starTreeAggregatedValuesMap.put(sc.getName(), new StarTreeAggregatedValues(null, dimensionValues,metricValues));
    }

    // TODO : fix this -> how to get rid of last param
  }

  @Override
  public void checkIntegrity()
      throws IOException {

  }

  @Override
  public DataCubeValues<StarTreeAggregatedValues> getDataCubeValues(String field)
      throws IOException {
    return new StarTreeDataCubeValues<StarTreeAggregatedValues>(starTreeAggregatedValuesMap.get(field));
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
    return new StarTreeAggregatedValues(starTree, dimensionValues, metricValues);
  }

  @Override
  public void close()
      throws IOException {
      if(valuesProducer != null) {
        valuesProducer.close();
      }
  }
}
