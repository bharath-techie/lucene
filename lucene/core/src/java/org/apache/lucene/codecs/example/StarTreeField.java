package org.apache.lucene.codecs.example;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.index.DataCubeFieldProvider;
import org.apache.lucene.index.DataCubeField;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;


public class StarTreeField extends DataCubeField {
  private final int maxLeafRecords;
  public StarTreeField(String name, Set<String> dims, Set<String> metrics, int maxLeafRecords) {
    super(name, dims, metrics);
    this.maxLeafRecords = maxLeafRecords;
  }

  @Override
  public String getProviderName() {
    return Provider.NAME;
  }

  public static final class Provider extends DataCubeFieldProvider {

    /** The name this Provider is registered under */
    public static final String NAME = "StarTreeDataCubeField";

    /** Creates a new Provider */
    public Provider() {
      super(NAME);
    }

    @Override
    public StarTreeField readDataCubeField(DataInput in) throws IOException {
      StarTreeField cf = new StarTreeField(
          in.readString(),
          in.readSetOfStrings(), in.readSetOfStrings(), in.readInt());

      return cf;
    }

    @Override
    public void writeDataCubeField(DataCubeField cf, DataOutput out) throws IOException {
      ((StarTreeField)cf).serialize(out);
    }

  }

  private void serialize(DataOutput out) throws IOException {
    out.writeString(getName());
    out.writeSetOfStrings(getDims());
    out.writeSetOfStrings(getMetrics());
    out.writeInt(maxLeafRecords);
  }
}
