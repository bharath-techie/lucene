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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.freshstartree.builder.BaseSingleTreeBuilder;
import org.apache.lucene.codecs.freshstartree.builder.OffHeapSingleTreeBuilder;
import org.apache.lucene.codecs.freshstartree.builder.OnHeapSingleTreeBuilder;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;

/** Custom star tree doc values writer */
public class StarTreeDocValuesWriter extends DocValuesConsumer {

  private DocValuesConsumer delegate;
  private final SegmentWriteState state;

  // TODO : should we make all of this final ?

  List<String> dimensionsSplitOrder;

  Map<String, SortedNumericDocValues> dimensionReaders;

  BaseSingleTreeBuilder builder;
  IndexOutput data;
  IndexOutput meta;

  DocValuesConsumer docValuesConsumer;

  public StarTreeDocValuesWriter(DocValuesConsumer delegate, SegmentWriteState segmentWriteState)
      throws IOException {
    this.delegate = delegate;
    this.state = segmentWriteState;
    dimensionReaders = new HashMap<>();
    dimensionsSplitOrder = new ArrayList<>();

    docValuesConsumer =
        new Lucene90DocValuesConsumer(state, DATA_CODEC, "sttd", META_CODEC, "sttm");
  }

  @Override
  public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    // TODO : check for attributes
    //    if(field.attributes().containsKey("dimensions") ||
    // field.attributes().containsKey("metric") ) {
    //      dimensionReaders.put(field.name, valuesProducer.getNumeric(field));
    //    }
    delegate.addNumericField(field, valuesProducer);
//    dimensionReaders.put(field.name + "_dim", valuesProducer.getNumeric(field));
//    if (field.name.contains("status")) {
//      // TODO : change this metric type
//      dimensionReaders.put(field.name + "_sum_metric", valuesProducer.getNumeric(field));
//    }
  }

  @Override
  public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    delegate.addBinaryField(field, valuesProducer);
  }

  @Override
  public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    delegate.addSortedField(field, valuesProducer);
  }

  @Override
  public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    delegate.addSortedNumericField(field, valuesProducer);
    dimensionReaders.put(field.name + "_dim", valuesProducer.getSortedNumeric(field));
    dimensionsSplitOrder.add(field.name);
    if (field.name.contains("status")) {
      // TODO : change this metric type
      dimensionReaders.put(field.name + "_sum_metric", valuesProducer.getSortedNumeric(field));
    }

    // TODO : later handle this for multiple star trees
//    if(field.attributes().containsKey("dimension")) {
//      dimensionReaders.put(field.name + "_dim", valuesProducer.getSortedNumeric(field));
//    }
//    if(field.attributes().containsKey("metric")) {
//      List<String> metrics = Arrays.asList(field.attributes().get("metric").split(","));
//      for(String metric : metrics) {
//        dimensionReaders.put(field.name + "_" + metric + "_metric", valuesProducer.getSortedNumeric(field));
//      }
//    }
  }

  @Override
  public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    delegate.addSortedSetField(field, valuesProducer);
  }

  @Override
  public void mergeAggregatedValues(MergeState mergeState) throws IOException {
    List<StarTreeAggregatedValues> aggrList = new ArrayList<>();
    for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
      DocValuesProducer producer = mergeState.docValuesProducers[i];
      Object obj = producer.getAggregatedDocValues();
      StarTreeAggregatedValues starTree = (StarTreeAggregatedValues) obj;
      aggrList.add(starTree);
    }
    // BaseSingleTreeBuilder.Record[] recordsArr = mergeRecords(aggrList);
//    // BaseSingleTreeBuilder.Record[] recordsArr = mergeRecords(aggrList);
//    dimensionsSplitOrder.add("hour");
//    dimensionsSplitOrder.add("day");
//    dimensionsSplitOrder.add("status");
    builder =
        new OnHeapSingleTreeBuilder(
            data,
            dimensionsSplitOrder,
            dimensionReaders,
            state.segmentInfo.maxDoc(),
            docValuesConsumer,
            state);
    //    long startTime = System.currentTimeMillis();
    // System.out.println(recordsArr);
    // TODO : remove this
    // todo: do this off heap
    builder.build(aggrList);
    // System.out.println("Finished merging star-tree in ms : " + (System.currentTimeMillis() -
    // startTime));
  }

  private BaseSingleTreeBuilder.Record[] mergeRecords(List<StarTreeAggregatedValues> aggrList)
      throws IOException {
    List<BaseSingleTreeBuilder.Record> records = new ArrayList<>();
    for (StarTreeAggregatedValues starTree : aggrList) {
      boolean endOfDoc = false;
      while (!endOfDoc) {
        int[] dims = new int[starTree.dimensionValues.size()];
        int i = 0;
        for (Map.Entry<String, NumericDocValues> dimValue : starTree.dimensionValues.entrySet()) {
          endOfDoc =
              dimValue.getValue().nextDoc() == DocIdSetIterator.NO_MORE_DOCS
                  || dimValue.getValue().longValue() == -1;
          if (endOfDoc) {
            break;
          }
          int val = (int) dimValue.getValue().longValue();
          dims[i] = val;
          i++;
        }
        if (endOfDoc) {
          break;
        }
        i = 0;
        Object[] metrics = new Object[starTree.metricValues.size()];
        for (Map.Entry<String, NumericDocValues> metricValue : starTree.metricValues.entrySet()) {
          metricValue.getValue().nextDoc();
          metrics[i] = metricValue.getValue().longValue();
          i++;
        }
        BaseSingleTreeBuilder.Record record = new BaseSingleTreeBuilder.Record(dims, metrics);
        // System.out.println("Adding  : " + record.toString());
        records.add(record);
      }
    }
    BaseSingleTreeBuilder.Record[] recordsArr = new BaseSingleTreeBuilder.Record[records.size()];
    records.toArray(recordsArr);
    records = null;
    return recordsArr;
  }

  @Override
  public void aggregate() throws IOException {
    // TODO : get this as input

    builder =
        new OffHeapSingleTreeBuilder(
            data,
            dimensionsSplitOrder,
            dimensionReaders,
            state.segmentInfo.maxDoc(),
            docValuesConsumer,
            state);
    builder.build();
  }

  @Override
  public void close() throws IOException {
    if (delegate != null) {
      delegate.close();
    }
    if (docValuesConsumer != null) {
      docValuesConsumer.close();
    }
    if (builder != null) {
      builder.close();
    }
  }
}
