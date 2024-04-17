package org.apache.lucene.index;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.NamedSPILoader;


public  abstract class DataCubeFieldProvider implements NamedSPILoader.NamedSPI {

  protected final String name;
  private static class Holder {
    private static final NamedSPILoader<DataCubeFieldProvider> LOADER =
        new NamedSPILoader<>(DataCubeFieldProvider.class);

    static NamedSPILoader<DataCubeFieldProvider> getLoader() {
      if (LOADER == null) {
        throw new IllegalStateException(
            "You tried to lookup a SortFieldProvider by name before all SortFieldProviders could be initialized. "
                + "This likely happens if you call SortFieldProvider#forName from a SortFieldProviders's ctor.");
      }
      return LOADER;
    }
  }

  public static DataCubeFieldProvider getProvider(DataCubeField cf) {
    return DataCubeFieldProvider.forName(cf.getName());
  }

  public static DataCubeFieldProvider forName(String name) {
    return DataCubeFieldProvider.Holder.getLoader().lookup(name);
  }

  public static Set<String> availableSortFieldProviders() {
    return DataCubeFieldProvider.Holder.getLoader().availableServices();
  }

  protected DataCubeFieldProvider(String name) {
    this.name = name;
  }


  public static void write(DataCubeField cf, DataOutput output) throws IOException {
    DataCubeFieldProvider provider = DataCubeFieldProvider.forName(cf.getProviderName());
    provider.writeDataCubeField(cf, output);
  }

  @Override
  public String getName() {
    return name;
  }

  public abstract DataCubeField readDataCubeField(DataInput in) throws IOException;

  public abstract void writeDataCubeField(DataCubeField cf,
      DataOutput out) throws IOException;
}
