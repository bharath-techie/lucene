package org.apache.lucene.codecs.example;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.index.CompositeFieldProvider;
import org.apache.lucene.index.CompositeIndexField;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;


public class StarTreeCompositeIndexField extends CompositeIndexField {

  private final Set<String> dims;
  private final Set<String> metrics;

  public StarTreeCompositeIndexField(String name, Set<String> fields, Set<String> dims, Set<String> metrics) {
    super(name, fields);
    this.dims = dims;
    this.metrics = metrics;
  }

  public Set<String> getDims() {
    return dims;
  }

  public Set<String> getMetrics() {
    return metrics;
  }

  @Override
  public String getProviderName() {
    return Provider.NAME;
  }

  public static final class Provider extends CompositeFieldProvider {

    /** The name this Provider is registered under */
    public static final String NAME = "StarTreeCompositeField";

    /** Creates a new Provider */
    public Provider() {
      super(NAME);
    }

    @Override
    public StarTreeCompositeIndexField readCompositeField(DataInput in) throws IOException {
      StarTreeCompositeIndexField cf = new StarTreeCompositeIndexField(
          in.readString(), in.readSetOfStrings(),
          in.readSetOfStrings(), in.readSetOfStrings());

      return cf;
    }

    @Override
    public void writeCompositeField(CompositeIndexField cf, DataOutput out) throws IOException {
      ((StarTreeCompositeIndexField)cf).serialize(out);
    }

  }

  private void serialize(DataOutput out) throws IOException {
    out.writeString(name);
    out.writeSetOfStrings(fields);
    out.writeSetOfStrings(dims);
    out.writeSetOfStrings(metrics);
  }
}
