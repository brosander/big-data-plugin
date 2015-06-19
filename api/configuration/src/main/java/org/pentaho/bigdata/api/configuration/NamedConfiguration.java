package org.pentaho.bigdata.api.configuration;

import java.util.Set;

/**
 * Created by bryan on 6/4/15.
 */
public interface NamedConfiguration extends ConfigurationNamespace {
  String getName();

  Set<String> getConfigurationNamespaces();

  ConfigurationNamespace getConfigurationNamespace( String namespace );
}
