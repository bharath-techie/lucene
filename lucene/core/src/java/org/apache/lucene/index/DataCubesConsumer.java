package org.apache.lucene.index;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.codecs.DataCubesProducer;


public abstract class DataCubesConsumer implements Closeable {

  /**
   * Method specific to composite values format
   */
  public abstract void flush(SegmentWriteState state, DataCubesConfig compositeConfig, LeafReader docValuesReader,
      Sorter.DocMap sortMap) throws IOException;

  public void merge(MergeState mergeState) throws IOException {
    // super.merge(mergeState);
    for (DataCubesProducer<?> docValuesProducer : mergeState.dataCubesReaders) {
      if (docValuesProducer != null) {
        docValuesProducer.checkIntegrity();
      }
    }
  }
}
