package org.apache.lucene.index;


public class CompositeConfig {
  private CompositeIndexField[] fields;

  public CompositeConfig(CompositeIndexField[] fields) {
    this.fields = fields;
  }

  public CompositeIndexField[] getFields() {
    return fields;
  }
}
