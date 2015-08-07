package org.pentaho.big.data.impl.shim.initializer;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.api.initializer.ClusterInitializerProvider;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.hadoop.shim.ConfigurationException;

/**
 * Created by bryan on 8/7/15.
 */
public class ClusterInitializerProviderImpl implements ClusterInitializerProvider {
  @Override public boolean canHandle( NamedCluster namedCluster ) {
    return true;
  }

  @Override public void initialize( NamedCluster namedCluster ) throws ClusterInitializationException {
    try {
      HadoopConfigurationBootstrap.getInstance().getProvider();
    } catch ( ConfigurationException e ) {
      throw new ClusterInitializationException( e );
    }
  }
}
