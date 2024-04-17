package org.apache.lucene.index;


public class DataCubesConfig {
  private DataCubeField[] fields;

  public DataCubesConfig(DataCubeField[] fields) {
    this.fields = fields;
  }

  public DataCubeField[] getFields() {
    return fields;
  }
}
