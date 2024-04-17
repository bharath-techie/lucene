package org.apache.lucene.codecs.example;

import java.io.IOException;
import org.apache.lucene.codecs.DataCubesFormat;
import org.apache.lucene.codecs.DataCubesProducer;
import org.apache.lucene.index.DataCubesConfig;
import org.apache.lucene.index.DataCubeDocValuesConsumer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;


public class ExampleDataCubesFormat extends DataCubesFormat {
  public ExampleDataCubesFormat() {
    super("Lucene90");
  }
  public static final String DATA_CODEC = "Lucene90DocValuesData";
  public static final String META_CODEC = "Lucene90DocValuesMetadata";
  @Override
  public DataCubesProducer<?> fieldsProducer(SegmentReadState state)
      throws IOException {
    return new ExampleDataCubesReader(state);
  }

  @Override
  public DataCubeDocValuesConsumer fieldsConsumer(SegmentWriteState state, DataCubesConfig compositeConfig)
      throws IOException {
    return new ExampleDataCubeDocValuesConsumer( state, DATA_CODEC, "sttd", META_CODEC, "sttd");
  }
}
