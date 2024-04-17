package org.apache.lucene.codecs.example;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public interface StarTree {

  /** Get the root node of the star tree. */
  StarTreeNode getRoot();

  /**
   * Get a list of all dimension names. The node dimension id is the index of the dimension name in
   * this list.
   */
  List<String> getDimensionNames();

  void printTree(Map<String, Map<String, String>> dictionaryMap) throws IOException;
}
