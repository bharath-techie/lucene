package org.apache.lucene.index;

public abstract class CompositeValues<T> {
  public abstract T getCompositeValues(String field);
}
