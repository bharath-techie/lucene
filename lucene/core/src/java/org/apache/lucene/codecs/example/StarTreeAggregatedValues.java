package org.apache.lucene.codecs.example;

import java.util.Map;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;


public class StarTreeAggregatedValues {
  public StarTree _starTree;
  public Map<String, SortedNumericDocValues> dimensionValues;
  public Map<String, SortedSetDocValues> keywordDimValues;
  public Map<String, SortedNumericDocValues> metricValues;

  public StarTreeAggregatedValues(
      StarTree starTree,
      Map<String, SortedNumericDocValues> dimensionValues,
      Map<String, SortedSetDocValues> keywordDimValues,
      Map<String, SortedNumericDocValues> metricValues
  ) {
    this._starTree = starTree;
    this.dimensionValues = dimensionValues;
    this.keywordDimValues = keywordDimValues;
    this.metricValues = metricValues;
  }
}
