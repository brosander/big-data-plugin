package org.pentaho.big.data.api.initializer.impl;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.api.initializer.ClusterInitializer;
import org.pentaho.big.data.api.initializer.ClusterInitializerProvider;

import java.util.List;

/**
 * Created by bryan on 8/7/15.
 */
public class ClusterInitializerImpl implements ClusterInitializer {
  private final List<ClusterInitializerProvider> providers;

  public ClusterInitializerImpl( List<ClusterInitializerProvider> providers ) {
    this.providers = providers;
  }

  @Override public void initialize( NamedCluster namedCluster ) throws ClusterInitializationException {
    for ( ClusterInitializerProvider provider : providers ) {
      if ( provider.canHandle( namedCluster ) ) {
        provider.initialize( namedCluster );
        return;
      }
    }
  }
}
