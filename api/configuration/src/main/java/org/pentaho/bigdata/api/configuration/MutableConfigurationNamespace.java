package org.pentaho.bigdata.api.configuration;

/**
 * Created by bryan on 6/18/15.
 */
public interface MutableConfigurationNamespace extends ConfigurationNamespace {
  String setProperty( String name, String value );
}
