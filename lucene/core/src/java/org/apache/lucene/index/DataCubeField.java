package org.apache.lucene.index;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;


public class DataCubeField {

  private final String name;
  private final Set<String> dims;
  private final Set<String> metrics;

  public DataCubeField(String name, Set<String> dims, Set<String> metrics) {
    this.name = name;
    this.dims = dims;
    this.metrics = metrics;
  }

  public Set<String> getDims() {
    return dims;
  }

  public Set<String> getMetrics() { return metrics; }

  public String getName() {
    return name;
  }

  public String getProviderName() {
    return Provider.NAME;
  }

  public static final class Provider extends DataCubeFieldProvider {

    /** The name this Provider is registered under */
    public static final String NAME = "DataCubeField";

    /** Creates a new Provider */
    public Provider() {
      super(NAME);
    }

    @Override
    public DataCubeField readDataCubeField(DataInput in) throws IOException {
      DataCubeField cf = new DataCubeField(in.readString(), in.readSetOfStrings(), in.readSetOfStrings());
      return cf;
    }

    @Override
    public void writeDataCubeField(DataCubeField cf, DataOutput out) throws IOException {
      cf.serialize(out);
    }

  }

  private void serialize(DataOutput out) throws IOException {
    out.writeString(name);
    out.writeSetOfStrings(dims);
    out.writeSetOfStrings(metrics);
  }

}
