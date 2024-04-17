package org.apache.lucene.codecs.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesProducer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DataCubeField;
import org.apache.lucene.index.DataCubesConfig;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;


public class StarTreeCustomDocValuesProducer extends Lucene90DocValuesProducer {
  /**
   * expert: instantiates a new reader
   *
   * @param state
   * @param dataCodec
   * @param dataExtension
   * @param metaCodec
   * @param metaExtension
   */
  public StarTreeCustomDocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension,
      String metaCodec, String metaExtension, DataCubesConfig starTreeConfig)
      throws IOException {
    super(state, dataCodec, dataExtension, metaCodec, metaExtension);
  }

  public NumericDocValues getNumeric(String field)
      throws IOException {
    FieldInfo fieldInfo = new FieldInfo(field, 0, false, false, true,
        IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, DocValuesType.NUMERIC, -1, Collections.emptyMap(),
        0, 0, 0, 0, VectorEncoding.FLOAT32, VectorSimilarityFunction.EUCLIDEAN, false);
    return super.getNumeric(fieldInfo);
  }

  @Override
  protected void readFields(IndexInput meta, FieldInfos infos, SegmentInfo segmentInfo) throws IOException {

    for(DataCubeField field : segmentInfo.getDataCubesConfig().getFields()) {
      List<String> docValueFields = new ArrayList<>();
      docValueFields.addAll(field.getDims());
      docValueFields.addAll(field.getMetrics());
      int i=0;
      for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
        readField(docValueFields.get(i), meta);
        i++;
      }
    }
  }
}
