package org.pentaho.big.data.api.cluster.service.locator;

import org.pentaho.big.data.api.cluster.NamedCluster;

/**
 * Created by bryan on 11/5/15.
 */
public interface NamedClusterServiceFactory<T> {
  Class<T> getServiceClass();
  boolean canHandle( NamedCluster namedCluster );
  T create( NamedCluster namedCluster );
}
