package org.apache.lucene.codecs.example;

import java.util.Map;
import org.apache.lucene.index.DataCubeValues;


public class StarTreeDataCubeValues<StarTreeAggregatedValues> extends DataCubeValues<StarTreeAggregatedValues> {

  StarTreeAggregatedValues starTreeAggregatedValues;
  public StarTreeDataCubeValues(StarTreeAggregatedValues starTreeAggregatedValues) {
    this.starTreeAggregatedValues = starTreeAggregatedValues;
  }
  @Override
  public StarTreeAggregatedValues getDataCubeValues() {
    return starTreeAggregatedValues;
  }
}
