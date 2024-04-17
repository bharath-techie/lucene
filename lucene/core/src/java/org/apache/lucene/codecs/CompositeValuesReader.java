package org.apache.lucene.codecs;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.index.CompositeValues;


public abstract class CompositeValuesReader<T> implements Closeable {

  protected CompositeValuesReader() {}
  public abstract void checkIntegrity() throws IOException;
  public abstract CompositeValues<?> getCompositeFieldValues(String field) throws IOException;

  public CompositeValuesReader<T> getMergeInstance() {
    return this;
  }

}
