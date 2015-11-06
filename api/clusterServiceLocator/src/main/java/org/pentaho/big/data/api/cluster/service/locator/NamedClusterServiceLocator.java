package org.pentaho.big.data.api.cluster.service.locator;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;

/**
 * Created by bryan on 11/5/15.
 */
public interface NamedClusterServiceLocator {
  <T> T getService( NamedCluster namedCluster, Class<T> serviceClass ) throws ClusterInitializationException;
}
