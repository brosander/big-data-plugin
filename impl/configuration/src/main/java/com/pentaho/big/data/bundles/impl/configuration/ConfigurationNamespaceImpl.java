package com.pentaho.big.data.bundles.impl.configuration;

import org.pentaho.bigdata.api.configuration.ConfigurationNamespace;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by bryan on 6/9/15.
 */
public class ConfigurationNamespaceImpl implements ConfigurationNamespace {
  private final Map<String, String> properties;

  public ConfigurationNamespaceImpl( Map<String, String> properties ) {
    this.properties = properties;
  }

  public Set<String> getProperties() {
    return Collections.unmodifiableSet( new HashSet<String>( properties.keySet() ) );
  }

  public String getProperty( String name ) {
    return properties.get( name );
  }

  public String getProperty( String name, String defaultValue ) {
    String property = properties.get( name );
    if ( property == null ) {
      return defaultValue;
    }
    return property;
  }
}
