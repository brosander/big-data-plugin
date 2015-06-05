package com.pentaho.big.data.bundles.impl.configuration;

import org.pentaho.bigdata.api.configuration.ConfigurationNamespace;
import org.pentaho.bigdata.api.configuration.NamedConfiguration;
import org.pentaho.bigdata.api.configuration.NamedConfigurationLocator;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by bryan on 6/9/15.
 */
public class NamedConfigurationLocatorImpl implements NamedConfigurationLocator {
  public NamedConfiguration get( String name ) {
    if ( "testConfig".equalsIgnoreCase( name ) ) {
      Map<String, String> properties = new HashMap<String, String>();
      Map<String, ConfigurationNamespace> configurationNamespaceMap = new HashMap<String, ConfigurationNamespace>();
      Map<String, String> hdfsProperties = new HashMap<String, String>();
      hdfsProperties.put( "fs.default.name", "hdfs://cdh52-rm:8020" );
      configurationNamespaceMap.put( "hdfs", new ConfigurationNamespaceImpl( hdfsProperties ) );
      return new NamedConfigurationImpl( properties, configurationNamespaceMap );
    }
    return null;
  }
}
