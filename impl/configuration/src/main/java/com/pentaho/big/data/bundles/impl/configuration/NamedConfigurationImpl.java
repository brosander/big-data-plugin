package com.pentaho.big.data.bundles.impl.configuration;

import org.pentaho.bigdata.api.configuration.ConfigurationNamespace;
import org.pentaho.bigdata.api.configuration.NamedConfiguration;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by bryan on 6/9/15.
 */
public class NamedConfigurationImpl extends ConfigurationNamespaceImpl implements NamedConfiguration {
  private final Map<String, ConfigurationNamespace> configurationNamespaceMap;

  public NamedConfigurationImpl( Map<String, String> properties,
                                 Map<String, ConfigurationNamespace> configurationNamespaceMap ) {
    super( properties );
    this.configurationNamespaceMap = configurationNamespaceMap;
  }

  public Set<String> getConfigurationNamespaces() {
    return Collections.unmodifiableSet( new HashSet<String>( configurationNamespaceMap.keySet() ) );
  }

  public ConfigurationNamespace getConfigurationNamespace( String namespace ) {
    return configurationNamespaceMap.get( namespace );
  }
}
