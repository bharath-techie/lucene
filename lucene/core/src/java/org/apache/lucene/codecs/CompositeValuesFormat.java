package org.apache.lucene.codecs;

import java.io.IOException;
import org.apache.lucene.index.CompositeConfig;
import org.apache.lucene.index.CompositeDocvaluesConsumer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.NamedSPILoader;


public abstract class CompositeValuesFormat implements NamedSPILoader.NamedSPI{

  private final String name;
  protected CompositeValuesFormat(String name) {
    NamedSPILoader.checkServiceName(name);
    this.name = name;
  }
  @Override
  public String getName() {
    return name;
  }

  public abstract CompositeValuesReader<?> fieldsReader(SegmentReadState state) throws IOException;

  public abstract CompositeDocvaluesConsumer fieldsConsumer(SegmentWriteState state, CompositeConfig compositeConfig) throws IOException;

  public static final CompositeValuesFormat EMPTY = new CompositeValuesFormat("EMPTY") {
    @Override
    public CompositeValuesReader<?> fieldsReader(SegmentReadState state)
        throws IOException {
      return null;
    }

    @Override
    public CompositeDocvaluesConsumer fieldsConsumer(SegmentWriteState state, CompositeConfig compositeConfig)
        throws IOException {
      throw new UnsupportedOperationException("Attempt to write EMPTY composite values");
    }
  };
}
