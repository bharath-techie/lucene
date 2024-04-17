package org.apache.lucene.codecs.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.codecs.CompositeValuesReader;
import org.apache.lucene.index.CompositeConfig;
import org.apache.lucene.index.CompositeDocvaluesConsumer;
import org.apache.lucene.index.CompositeIndexField;
import org.apache.lucene.index.CompositeValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;


public class ExampleCompositeDocValuesConsumer extends CompositeDocvaluesConsumer {
  @Override
  public void flush(CompositeConfig compositeConfig) throws IOException {
    StarTreeCompositeConfig config = (StarTreeCompositeConfig) compositeConfig;
    System.out.println();
    System.out.println("======= In flush : ======");
    for(CompositeIndexField field : config.getFields()){
      System.out.println("Composite index Field name : " + field.getName());
      System.out.println("Metrics : " + Arrays.toString(((StarTreeCompositeIndexField)field).getMetrics().toArray()));
      System.out.println("Dims : " + Arrays.toString(((StarTreeCompositeIndexField)field).getDims().toArray()));

      System.out.println("======== Reading metrics ========");

      for(String metric : ((StarTreeCompositeIndexField)field).getMetrics()){
        System.out.println("Field name: " + metric);
        SortedNumericDocValues values = DocValues.singleton(getNumericDocValuesMap().get(metric));
        int docID;
        while ((docID = values.nextDoc()) != NO_MORE_DOCS) {
          final int count = values.docValueCount();
          for (int i = 0; i < count; ++i) {
            final long v = values.nextValue();
            System.out.print(v + " ");
          }
        }
      }
      System.out.println();
      System.out.println("======== Reading dims ========");

      for(String dim : ((StarTreeCompositeIndexField)field).getDims()){
        System.out.println("Field name: " + dim);
        SortedNumericDocValues values = DocValues.singleton(getNumericDocValuesMap().get(dim));
        int docID;
        while ((docID = values.nextDoc()) != NO_MORE_DOCS) {
          final int count = values.docValueCount();
          for (int i = 0; i < count; ++i) {
            final long v = values.nextValue();
            System.out.print(v + " ");
          }
        }
        System.out.println();
      }
    }

    // fix this
//    builder.flush(docValuesConsumer, state

  }

  @Override
  public void merge(MergeState mergeState) throws IOException {
    super.merge(mergeState);
    List<StarTreeAggregatedValues> aggrList = new ArrayList<>();
    List<String> dimNames = new ArrayList<>();
    for (int i = 0; i < mergeState.compositeValuesReaders.length; i++) {
      CompositeValuesReader<?> producer = mergeState.compositeValuesReaders[i];
     // if(mergeState.compositeValuesReaders[i] )
      @SuppressWarnings("unchecked")
      CompositeValues<StarTreeAggregatedValues> starTree =
          (CompositeValues<StarTreeAggregatedValues>) producer.getCompositeFieldValues("field_place_holder");

      //dimNames = starTree.getCompositeValues("field_place_holder").dimensionValues.keySet().stream().collect(Collectors.toList());
      //aggrList.add(starTree);
    }
    long startTime = System.currentTimeMillis();
    // fix this
//    builder = new OffHeapSingleTreeBuilder(
//        data,
//        dimNames,
//        dimensionReaders,
//        textDimensionReaders,
//        state.segmentInfo.maxDoc(),
//        docValuesConsumer,
//        state
//    );
//    builder.build(aggrList, mergeState);
//    logger.info("Finished merging star-tree in ms : {}", (System.currentTimeMillis() - startTime));
  }


}
