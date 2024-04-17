package org.apache.lucene.codecs.example;

import org.apache.lucene.index.DataCubesConfig;


public class StarTreeConfig extends DataCubesConfig {

  StarTreeField[] fields;
  public StarTreeConfig(StarTreeField[] fields) {

    super(fields);
    this.fields = fields;
  }

  @Override
  public StarTreeField[] getFields() {
    return fields;
  }
}
