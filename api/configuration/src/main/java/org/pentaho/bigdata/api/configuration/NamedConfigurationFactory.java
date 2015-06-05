package org.pentaho.bigdata.api.configuration;

/**
 * Created by bryan on 6/18/15.
 */
public interface NamedConfigurationFactory {
  MutableNamedConfiguration create( String name );
}
