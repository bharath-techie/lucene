package org.apache.lucene.codecs.example;

import java.io.IOException;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.packed.PackedLongValues;


public class BufferedAggregatedDocValues extends NumericDocValues {
  final PackedLongValues.Iterator iter;
  final DocIdSetIterator docsWithField;
  private long value;

  public BufferedAggregatedDocValues(PackedLongValues values, DocIdSetIterator docsWithFields) {
    this.iter = values.iterator();
    this.docsWithField = docsWithFields;
  }

  @Override
  public int docID() {
    return docsWithField.docID();
  }

  @Override
  public int nextDoc() throws IOException {


    int docID = docsWithField.nextDoc();
    if (docID != NO_MORE_DOCS) {
      value = iter.next();
    }
    return docID;
  }

  @Override
  public int advance(int target) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean advanceExact(int target) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long cost() {
    return docsWithField.cost();
  }

  @Override
  public long longValue() {
    return value;
  }
}

