package org.apache.lucene.codecs.example;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.CompositeConfig;
import org.apache.lucene.index.CompositeIndexField;


public class StarTreeCompositeConfig extends CompositeConfig {
  public StarTreeCompositeConfig(StarTreeCompositeIndexField[] fields) {
    super(fields);
  }
}
