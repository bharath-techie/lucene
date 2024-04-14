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
package org.apache.lucene.codecs.freshstartree.query;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.lucene.codecs.freshstartree.codec.StarTreeAggregatedValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
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

/** Query class for querying star tree data structure */
public class StarTreeQuery extends Query implements Accountable {

  Map<String, List<Predicate<Integer>>> compositePredicateMap;
  Set<String> groupByColumns;

  public StarTreeQuery(
      Map<String, List<Predicate<Integer>>> compositePredicateMap, Set<String> groupByColumns) {
    this.compositePredicateMap = compositePredicateMap;
    this.groupByColumns = groupByColumns;
  }

  @Override
  public String toString(String field) {
    return null;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
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
    return 0;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {

    return new ConstantScoreWeight(this, boost) {
      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        StarTreeAggregatedValues val = null;
        DocIdSetIterator result = null;
        Object obj = context.reader().getAggregatedDocValues();
        if (obj != null) {
          val = (StarTreeAggregatedValues) obj;
          StarTreeFilter filter = new StarTreeFilter(val, compositePredicateMap, groupByColumns);
          result = filter.getStarTreeResult();
        }
        return new ConstantScoreScorer(this, score(), scoreMode, result);
      }

      @Override
      public int count(LeafReaderContext context) throws IOException {
        StarTreeAggregatedValues val = null;
        DocIdSetIterator result = null;
        Object obj = context.reader().getAggregatedDocValues();
        if (obj != null) {
          val = (StarTreeAggregatedValues) obj;
          StarTreeFilter filter = new StarTreeFilter(val, compositePredicateMap, groupByColumns);
          result = filter.getStarTreeResult();
        }


        LeafReader reader = context.reader();

        PointValues values = reader.getPointValues(field);
        if (checkValidPointValues(values) == false) {
          return 0;
        }

        if (reader.hasDeletions() == false) {
          if (relate(values.getMinPackedValue(), values.getMaxPackedValue())
              == PointValues.Relation.CELL_INSIDE_QUERY) {
            return values.getDocCount();
          }
          // only 1D: we have the guarantee that it will actually run fast since there are at most 2
          // crossing leaves.
          // docCount == size : counting according number of points in leaf node, so must be
          // single-valued.
          if (numDims < 4 && values.getDocCount() == values.size()) {
            return (int) pointCount(values.getPointTree(), this::relate, this::matches);
          }
        }
        return super.count(context);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return false;
      }
    };
  }
}
