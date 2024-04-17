package org.apache.lucene.codecs;

import java.io.IOException;
import org.apache.lucene.index.DataCubesConfig;
import org.apache.lucene.index.DataCubeDocValuesConsumer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.NamedSPILoader;


public abstract class DataCubesFormat implements NamedSPILoader.NamedSPI{

  private final String name;
  protected DataCubesFormat(String name) {
    NamedSPILoader.checkServiceName(name);
    this.name = name;
  }
  @Override
  public String getName() {
    return name;
  }

  public abstract DataCubesProducer<?> fieldsProducer(SegmentReadState state) throws IOException;

  public abstract DataCubeDocValuesConsumer fieldsConsumer(SegmentWriteState state, DataCubesConfig compositeConfig) throws IOException;

  public static final DataCubesFormat EMPTY = new DataCubesFormat("EMPTY") {
    @Override
    public DataCubesProducer<?> fieldsProducer(SegmentReadState state)
        throws IOException {
      return null;
    }

    @Override
    public DataCubeDocValuesConsumer fieldsConsumer(SegmentWriteState state, DataCubesConfig compositeConfig)
        throws IOException {
      throw new UnsupportedOperationException("Attempt to write EMPTY composite values");
    }
  };
}
