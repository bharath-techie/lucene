package org.apache.lucene.index;

import java.io.IOException;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompositeValuesFormat;
import org.apache.lucene.codecs.CompositeValuesWriter;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;


public class CompositeValuesConsumer {
  private final Codec codec;
  private final Directory directory;
  private final SegmentInfo segmentInfo;
  private CompositeValuesWriter<?> writer;
  private final InfoStream infoStream;

  CompositeValuesConsumer(Codec codec, Directory directory, SegmentInfo segmentInfo, InfoStream infoStream) {
    this.codec = codec;
    this.directory = directory;
    this.segmentInfo = segmentInfo;
    this.infoStream = infoStream;
  }

  private void initCompositeValuesFormat(String fieldName) throws IOException {
    if (writer == null) {
      CompositeValuesFormat fmt = codec.compositeValuesFormat();
      if (fmt == null) {
        throw new IllegalStateException(
            "field=\""
                + fieldName
                + "\" was indexed as vectors but codec does not support vectors");
      }
      SegmentWriteState initialWriteState =
          new SegmentWriteState(infoStream, directory, segmentInfo, null, null, IOContext.DEFAULT);
      //writer = fmt.fieldsWriter(initialWriteState);
      //accountable = writer;
    }
  }

  public CompositeValuesWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    initCompositeValuesFormat(fieldInfo.name);
    return writer.addField(fieldInfo);
  }

  void flush(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    if (writer == null) return;
    try {
      writer.flush(state, sortMap);
      writer.finish();
    } finally {
      IOUtils.close(writer);
    }
  }

}
