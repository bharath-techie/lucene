package org.apache.lucene.document;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesRangeIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.search.*;
import org.apache.lucene.index.*;
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.IOException;
import java.util.Objects;

public class RangeBitmapQuery extends Query {
    private final String field;
    private final long min;
    private final long max;
    private final int numDocs;

    public RangeBitmapQuery(String field, long min, long max, int numDocs) {
        this.field = Objects.requireNonNull(field);
        this.min = min;
        this.max = max;
        this.numDocs = numDocs;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
            throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    ImmutableRoaringBitmap matchingDocs = getMatchingDocIds(context.reader().getRangeBitMap());

                    if (matchingDocs.isEmpty()) {
                        return null;
                    }

                    DocIdSetIterator approximation = new DocIdSetIterator() {
                        private int current = -1;
                        private final org.roaringbitmap.IntIterator iterator =
                                matchingDocs.getIntIterator();

                        @Override
                        public int docID() {
                            return current;
                        }

                        @Override
                        public int nextDoc() throws IOException {
                            if (iterator.hasNext()) {
                                current = iterator.next();
                                return current;
                            }
                            return current = NO_MORE_DOCS;
                        }

                        @Override
                        public int advance(int target) throws IOException {
                            if (!iterator.hasNext()) {
                                return current = NO_MORE_DOCS;
                            }
                            current = iterator.next();
                            while (current < target && iterator.hasNext()) {
                                current = iterator.next();
                            }
                            return current < target ? NO_MORE_DOCS : current;
                        }

                        @Override
                        public long cost() {
                            return matchingDocs.getCardinality();
                        }
                    };

                final var scorer = new ConstantScoreScorer(score(), scoreMode, approximation);
                return new DefaultScorerSupplier(scorer);

            }
        };
    }

    private ImmutableRoaringBitmap getMatchingDocIds(RangeBitmap rangeBitmap) {
        if (min > max) {
            return new MutableRoaringBitmap();
        }

        if (min-min == max-min) {
            return rangeBitmap.eq(min-min).toMutableRoaringBitmap();
        }

        return rangeBitmap.between(min-min, max-min).toMutableRoaringBitmap();
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String field) {
        return String.format("%s:[%d TO %d]", this.field, min, max);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RangeBitmapQuery)) return false;
        RangeBitmapQuery that = (RangeBitmapQuery) o;
        return min == that.min &&
                max == that.max &&
                numDocs == that.numDocs &&
                Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, min, max, numDocs);
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        if (min > max) {
            return new MatchNoDocsQuery();
        }
        return super.rewrite(searcher);
    }
}