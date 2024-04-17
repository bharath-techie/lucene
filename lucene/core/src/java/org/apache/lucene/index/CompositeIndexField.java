package org.apache.lucene.index;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;


public class CompositeIndexField {

  public String name;
  public Set<String> fields;

  public CompositeIndexField(String name, Set<String> fields) {
    this.name = name;
    this.fields = fields;
  }

  public Set<String> getFields() {
    return fields;
  }

  public String getName() {
    return name;
  }

  public String getProviderName() {
    return Provider.NAME;
  }

  public static final class Provider extends CompositeFieldProvider {

    /** The name this Provider is registered under */
    public static final String NAME = "CompositeField";

    /** Creates a new Provider */
    public Provider() {
      super(NAME);
    }

    @Override
    public CompositeIndexField readCompositeField(DataInput in) throws IOException {
      CompositeIndexField cf = new CompositeIndexField(in.readString(), in.readSetOfStrings());

      return cf;
    }

    @Override
    public void writeCompositeField(CompositeIndexField cf, DataOutput out) throws IOException {
      cf.serialize(out);
    }

  }

  private void serialize(DataOutput out) throws IOException {
    out.writeString(name);
    out.writeSetOfStrings(fields);
  }

}
