package org.pentaho.bigdata.api.configuration;

/**
 * Created by bryan on 6/18/15.
 */
public interface MutableNamedConfiguration extends NamedConfiguration, MutableConfigurationNamespace {
  MutableConfigurationNamespace createConfigurationNamespace( String namespace );
}
