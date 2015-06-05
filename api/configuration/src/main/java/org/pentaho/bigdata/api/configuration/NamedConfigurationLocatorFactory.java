package org.pentaho.bigdata.api.configuration;

import org.pentaho.metastore.api.IMetaStore;

/**
 * Created by bryan on 6/18/15.
 */
public interface NamedConfigurationLocatorFactory {
  NamedConfigurationLocator create( IMetaStore metaStore );
}
