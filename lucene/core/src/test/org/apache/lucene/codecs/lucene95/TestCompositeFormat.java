
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
package org.apache.lucene.codecs.lucene95;


import java.io.IOException;

import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.example.StarTreeCompositeConfig;
import org.apache.lucene.codecs.example.StarTreeCompositeIndexField;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CompositeValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class TestCompositeFormat extends LuceneTestCase {
  private static final String VECTOR_FIELD = "vector";

  public void testBasicIndexing() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig c =  newIndexWriterConfig().setCodec(Codec.forName("Lucene95"));
    Set<String> fields = new HashSet<>();
    Set<String> dims = new HashSet<>();
    Set<String> metrics = new HashSet<>();
    fields = Set.of("status", "clientip", "targetip");
    dims = Set.of("clientip", "targetip");
    metrics = Set.of("status");
    StarTreeCompositeIndexField field = new StarTreeCompositeIndexField("composite", fields, dims, metrics);
    StarTreeCompositeIndexField[] fieldArr = new StarTreeCompositeIndexField[]{field};
    StarTreeCompositeConfig starConfig = new StarTreeCompositeConfig(fieldArr);
    c.setCompositeConfig(starConfig);
    IndexWriter writer = new IndexWriter(dir, c);

    int numDoc = 10;
    add(writer);
    writer.commit();
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    for (LeafReaderContext leafReaderContext : reader.leaves()) {
      CompositeValues<?> vectorValues = leafReaderContext.reader().getCompositeValues(VECTOR_FIELD);
      //assertNotEquals(vectorValues.iterator().nextDoc(), NO_MORE_DOCS);
    }

    reader.close();
    dir.close();
  }

  private float[] randomVector(int dim) {
    float[] vector = new float[dim];
    for (int i = 0; i < vector.length; i++) {
      vector[i] = random().nextFloat();
    }
    return vector;
  }

  private void add(IndexWriter iw) throws IOException {

    final int numDocs = 30;//;atLeast(200);
    final long[] values = new long[TestUtil.nextInt(random(), 1, 500)];
    final int maxNumValuesPerDoc = random().nextBoolean() ? 1 : TestUtil.nextInt(random(), 2, 5);
    System.out.println("===== Adding docs =========");
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      System.out.print(i+ " ");
      doc.add(new NumericDocValuesField("status", i));
      doc.add(new NumericDocValuesField("clientip", i));
      doc.add(new NumericDocValuesField("targetip", i));
//      doc.add(new SortedDocValuesField("sorted", new BytesRef(Long.toString(docValue))));
//      doc.add(new BinaryDocValuesField("binary", new BytesRef(Long.toString(docValue))));
//      doc.add(new StoredField("value", docValue));

      iw.addDocument(doc);
    }
  }
}