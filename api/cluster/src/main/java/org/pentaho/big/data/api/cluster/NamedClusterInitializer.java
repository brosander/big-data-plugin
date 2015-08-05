package org.pentaho.big.data.api.cluster;

/**
 * Created by bryan on 8/5/15.
 */
public interface NamedClusterInitializer {
  boolean init( NamedCluster namedCluster ) throws NamedClusterInitializationException;
}
