package org.apache.lucene.index;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.codecs.DataCubesProducer;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import java.util.Map;


public class DataCubeDocValuesConsumer extends DocValuesConsumer {

  Map<String, NumericDocValues> numericDocValuesMap = new ConcurrentHashMap<>();
  Map<String, BinaryDocValues> binaryDocValuesMap = new ConcurrentHashMap<>();
  Map<String, SortedDocValues> sortedDocValuesMap = new ConcurrentHashMap<>();
  Map<String, SortedNumericDocValues> sortedNumericDocValuesMap = new ConcurrentHashMap<>();
  Map<String, SortedSetDocValues> sortedSetDocValuesMap = new ConcurrentHashMap<>();

  /**
   *
   */
  public DataCubeDocValuesConsumer()
      throws IOException {
    super();
  }

  @Override
  public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
   numericDocValuesMap.put(field.name, valuesProducer.getNumeric(field));
  }

  @Override
  public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    binaryDocValuesMap.put(field.name, valuesProducer.getBinary(field));
  }

  @Override
  public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    sortedDocValuesMap.put(field.name, valuesProducer.getSorted(field));
  }

  @Override
  public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    sortedNumericDocValuesMap.put(field.name, valuesProducer.getSortedNumeric(field));
  }

  @Override
  public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    sortedSetDocValuesMap.put(field.name, valuesProducer.getSortedSet(field));
  }

  public Map<String, NumericDocValues> getNumericDocValuesMap() {
    return numericDocValuesMap;
  }

  public Map<String, BinaryDocValues> getBinaryDocValuesMap() {
    return binaryDocValuesMap;
  }

  public Map<String, SortedDocValues> getSortedDocValuesMap() {
    return sortedDocValuesMap;
  }

  public Map<String, SortedNumericDocValues> getSortedNumericDocValuesMap() {
    return sortedNumericDocValuesMap;
  }

  public Map<String, SortedSetDocValues> getSortedSetDocValuesMap() {
    return sortedSetDocValuesMap;
  }
  @Override
  public void close()
      throws IOException {

  }

  /**
   * Method specific to composite values format
   */
  public void flush(DataCubesConfig compositeConfig) throws IOException {

  }

  @Override
  public void merge(MergeState mergeState) throws IOException {
    // super.merge(mergeState);
    for (DataCubesProducer<?> docValuesProducer : mergeState.dataCubesReaders) {
      if (docValuesProducer != null) {
        docValuesProducer.checkIntegrity();
      }
    }
  }
}
