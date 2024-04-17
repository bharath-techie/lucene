
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

import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.example.StarTreeAggregatedValues;
import org.apache.lucene.codecs.example.StarTreeConfig;
import org.apache.lucene.codecs.example.StarTreeField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DataCubeValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;


public class TestDataCubesFormat extends LuceneTestCase {

  public void testBasicIndexing() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig c =  newIndexWriterConfig().setCodec(Codec.forName("Lucene95"));
    Set<String> dims = Set.of("clientip", "targetip");
    Set<String> metrics = Set.of("status");
    StarTreeField field = new StarTreeField("status-target-client", dims, metrics, 1000);
    StarTreeField[] fieldArr = new StarTreeField[]{field};
    StarTreeConfig starConfig = new StarTreeConfig(fieldArr);
    c.setDataCubesConfig(starConfig);
    IndexWriter writer = new IndexWriter(dir, c);

    add(writer);
    writer.commit();
    //writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    for (LeafReaderContext leafReaderContext : reader.leaves()) {
      DataCubeValues<?> dataCubeValues = leafReaderContext.reader().getDataCubeValues("status-target-client");
      if(dataCubeValues != null) {
        StarTreeAggregatedValues res = (StarTreeAggregatedValues) dataCubeValues.getDataCubeValues();
        for(String key : res.dimensionValues.keySet()) {
          System.out.println("Dim : " + key);
          NumericDocValues ndv = res.dimensionValues.get(key);
          for (int i = 0; i < leafReaderContext.reader().maxDoc(); i++) {
            assertEquals(i, ndv.nextDoc());
            assertEquals(i, ndv.longValue());
          }
        }
        for(String key : res.metricValues.keySet()) {
          System.out.println("Metric : "  + key);
          NumericDocValues ndv = res.metricValues.get(key);
          for (int i = 0; i < leafReaderContext.reader().maxDoc(); i++) {
            assertEquals(i, ndv.nextDoc());
            assertEquals(i*2, ndv.longValue());
          }
        }
      }
      //assertNotEquals(vectorValues.iterator().nextDoc(), NO_MORE_DOCS);
    }

    reader.close();
    dir.close(); //TODO: this fails
  }

  private void add(IndexWriter iw) throws IOException {

    final int numDocs = 30;
    System.out.println("===== Adding docs =========");
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      System.out.print(i+ " ");
      doc.add(new NumericDocValuesField("status", i));
      doc.add(new NumericDocValuesField("clientip", i));
      doc.add(new NumericDocValuesField("targetip", i));
      iw.addDocument(doc);
    }
  }
}