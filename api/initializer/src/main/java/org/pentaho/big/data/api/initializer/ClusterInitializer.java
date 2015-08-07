package org.pentaho.big.data.api.initializer;

import org.pentaho.big.data.api.cluster.NamedCluster;

/**
 * Created by bryan on 8/7/15.
 */
public interface ClusterInitializer {
  void initialize( NamedCluster namedCluster ) throws ClusterInitializationException;
}
