package org.apache.lucene.codecs;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.index.DataCubeValues;


public abstract class DataCubesProducer<T> implements Closeable {

  protected DataCubesProducer() {}
  public abstract void checkIntegrity() throws IOException;
  public abstract DataCubeValues<?> getDataCubeValues(String field) throws IOException;

  public DataCubesProducer<T> getMergeInstance() {
    return this;
  }

}
