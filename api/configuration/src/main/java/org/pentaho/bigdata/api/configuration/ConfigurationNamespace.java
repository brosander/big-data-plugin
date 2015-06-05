package org.pentaho.bigdata.api.configuration;

import java.util.Set;

/**
 * Created by bryan on 6/4/15.
 */
public interface ConfigurationNamespace {
  Set<String> getProperties();

  String getProperty( String name );

  String getProperty( String name, String defaultValue );
}
