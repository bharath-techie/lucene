package org.apache.lucene.codecs;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.util.Accountable;


public abstract class CompositeValuesWriter<T> implements Accountable, Closeable {

  private final DocValuesConsumer docValuesWriter;

  public CompositeValuesWriter(SegmentWriteState state)
      throws IOException {
    docValuesWriter = new Lucene90DocValuesFormat().fieldsConsumer(state);
  }

  public abstract CompositeValuesWriter<?> addField(FieldInfo field) throws IOException;

  public void merge(MergeState mergeState) throws IOException {

  }
  public abstract void finish() throws IOException;

  public void flush(SegmentWriteState state, Sorter.DocMap sortMap) {
    
  }
}
