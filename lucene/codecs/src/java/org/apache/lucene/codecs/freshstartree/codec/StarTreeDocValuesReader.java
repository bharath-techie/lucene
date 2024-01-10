/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs.freshstartree.codec;

import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.DATA_CODEC;
import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.META_CODEC;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.freshstartree.node.OffHeapStarTree;
import org.apache.lucene.codecs.freshstartree.node.StarTree;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.IndexInput;

/** Custom star tree doc values reader */
public class StarTreeDocValuesReader extends DocValuesProducer {
  private DocValuesProducer delegate;

  private IndexInput data;

  private Lucene90DocValuesProducerCp valuesProducer;

  StarTree starTree;

  Map<String, NumericDocValues> dimensionValues;

  Map<String, NumericDocValues> metricValues;

  public StarTreeDocValuesReader(DocValuesProducer producer, SegmentReadState state)
      throws IOException {
    this.delegate = producer;
    String dataName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "stttree");
    this.data = state.directory.openInput(dataName, state.context);
    CodecUtil.checkIndexHeader(
        data,
        "STARTreeCodec",
        0,
        Lucene90DocValuesFormat.VERSION_CURRENT,
        state.segmentInfo.getId(),
        state.segmentSuffix);
    starTree = new OffHeapStarTree(data);
    dimensionValues = new HashMap<>();
    valuesProducer =
        new Lucene90DocValuesProducerCp(state, DATA_CODEC, "sttd",
            META_CODEC, "sttm", starTree.getDimensionNames());

  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    return delegate.getNumeric(field);
  }

  @Override
  public StarTreeAggregatedValues getAggregatedDocValues() throws IOException {
    //    starTree.printTree(new HashMap<>());
    //    System.out.println(starTree);
    List<String> dimensionsSplitOrder = starTree.getDimensionNames();
    for (int i = 0; i < dimensionsSplitOrder.size(); i++) {
      dimensionValues.put(
          dimensionsSplitOrder.get(i),
          valuesProducer.getNumeric(dimensionsSplitOrder.get(i) + "_dim"));
    }
    metricValues = new HashMap<>();
    metricValues.put("status_sum", valuesProducer.getNumeric("status_sum_metric"));
    metricValues.put("status_count", valuesProducer.getNumeric("status_count_metric"));

    return new StarTreeAggregatedValues(starTree, dimensionValues, metricValues);
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    return delegate.getBinary(field);
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    return delegate.getSorted(field);
  }

  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    return delegate.getSortedNumeric(field);
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    return delegate.getSortedSet(field);
  }

  @Override
  public void checkIntegrity() throws IOException {}

  @Override
  public void close() throws IOException {}
}
