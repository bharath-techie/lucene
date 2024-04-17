package org.apache.lucene.codecs.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.lucene.index.DataCubeValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;


public class DataCubesQuery extends Query implements Accountable {

  private final String field;

  private final Map<String, List<Predicate<Long>>> filterPredicateMap = new HashMap<>();

  private final Set<String> groupByCols = new HashSet<>();

  public DataCubesQuery(String field) {
    this.field = field;
  }

  @Override
  public String toString(String field) {
    return null;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this); // todo
  }

  @Override
  public boolean equals(Object obj) {
    return sameClassAs(obj);
  }

  @Override
  public int hashCode() {
    return classHash();
  }

  @Override
  public long ramBytesUsed() {
    return 0; // Todo
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {
      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        StarTreeAggregatedValues starTreeValues = null;
        DocIdSetIterator result = null;
        DataCubeValues<?> dataCubVals = context.reader().getDataCubeValues(field);
        Object dataCubeValues = dataCubVals.getDataCubeValues();
        if (dataCubeValues != null) {
          starTreeValues = (StarTreeAggregatedValues) dataCubeValues;
          StarTreeFilter filter = new StarTreeFilter(starTreeValues, filterPredicateMap, groupByCols);
          // Traverse star tree and get the resultant DocIdSetIterator of star tree + star tree doc values
          result = filter.getStarTreeResult();
        }
        return new ConstantScoreScorer(this, score(), scoreMode, result);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return false;
      }
    };
  }
}
