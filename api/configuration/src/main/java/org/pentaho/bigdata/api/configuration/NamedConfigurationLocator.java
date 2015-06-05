package org.pentaho.bigdata.api.configuration;

/**
 * Created by bryan on 6/4/15.
 */
public interface NamedConfigurationLocator {
  NamedConfiguration get( String uri );
}
