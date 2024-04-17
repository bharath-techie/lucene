package org.apache.lucene.index;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.NamedSPILoader;


public  abstract class CompositeFieldProvider implements NamedSPILoader.NamedSPI {

  protected final String name;
  private static class Holder {
    private static final NamedSPILoader<CompositeFieldProvider> LOADER =
        new NamedSPILoader<>(CompositeFieldProvider.class);

    static NamedSPILoader<CompositeFieldProvider> getLoader() {
      if (LOADER == null) {
        throw new IllegalStateException(
            "You tried to lookup a SortFieldProvider by name before all SortFieldProviders could be initialized. "
                + "This likely happens if you call SortFieldProvider#forName from a SortFieldProviders's ctor.");
      }
      return LOADER;
    }
  }

  public static CompositeFieldProvider getProvider(CompositeIndexField cf) {
    return CompositeFieldProvider.forName(cf.getName());
  }

  public static CompositeFieldProvider forName(String name) {
    return CompositeFieldProvider.Holder.getLoader().lookup(name);
  }

  public static Set<String> availableSortFieldProviders() {
    return CompositeFieldProvider.Holder.getLoader().availableServices();
  }

  protected CompositeFieldProvider(String name) {
    this.name = name;
  }


  public static void write(CompositeIndexField cf, DataOutput output) throws IOException {
    CompositeFieldProvider provider = CompositeFieldProvider.forName(cf.getProviderName());
    provider.writeCompositeField(cf, output);
  }

  @Override
  public String getName() {
    return name;
  }

  public abstract CompositeIndexField readCompositeField(DataInput in) throws IOException;

  public abstract void writeCompositeField(CompositeIndexField cf,
      DataOutput out) throws IOException;
}
