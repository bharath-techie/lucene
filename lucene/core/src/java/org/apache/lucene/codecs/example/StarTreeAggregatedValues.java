package org.apache.lucene.codecs.example;

import java.util.Map;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;


public class StarTreeAggregatedValues {
  public StarTree _starTree;
  public Map<String, NumericDocValues> dimensionValues;
  public Map<String, NumericDocValues> metricValues;

  public StarTreeAggregatedValues(
      StarTree starTree,
      Map<String,  NumericDocValues> dimensionValues,
      Map<String, NumericDocValues> metricValues
  ) {
    this._starTree = starTree;
    this.dimensionValues = dimensionValues;
    this.metricValues = metricValues;
  }
}
