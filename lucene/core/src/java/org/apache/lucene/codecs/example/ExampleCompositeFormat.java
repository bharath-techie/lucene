package org.apache.lucene.codecs.example;

import java.io.IOException;
import org.apache.lucene.codecs.CompositeValuesFormat;
import org.apache.lucene.codecs.CompositeValuesReader;
import org.apache.lucene.index.CompositeConfig;
import org.apache.lucene.index.CompositeDocvaluesConsumer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;


public class ExampleCompositeFormat extends CompositeValuesFormat {
  public ExampleCompositeFormat() {
    super("Lucene90");
  }

  @Override
  public CompositeValuesReader<?> fieldsReader(SegmentReadState state)
      throws IOException {
    return new ExampleCompositeValuesReader(state);
  }

  @Override
  public CompositeDocvaluesConsumer fieldsConsumer(SegmentWriteState state, CompositeConfig compositeConfig)
      throws IOException {
    return new ExampleCompositeDocValuesConsumer();
  }
}
