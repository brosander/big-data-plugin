package org.pentaho.big.data.impl.shim.cluster;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterInitializationException;
import org.pentaho.big.data.api.cluster.NamedClusterInitializer;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.hadoop.shim.ConfigurationException;

/**
 * Created by bryan on 8/5/15.
 */
public class NamedClusterInitializerShimImpl implements NamedClusterInitializer {
  private final HadoopConfigurationBootstrap hadoopConfigurationBootstrap;

  public NamedClusterInitializerShimImpl() {
    this( HadoopConfigurationBootstrap.getInstance() );
  }

  public NamedClusterInitializerShimImpl( HadoopConfigurationBootstrap hadoopConfigurationBootstrap ) {
    this.hadoopConfigurationBootstrap = hadoopConfigurationBootstrap;
  }

  @Override public boolean init( NamedCluster namedCluster ) throws NamedClusterInitializationException {
    try {
      hadoopConfigurationBootstrap.getProvider();
    } catch ( ConfigurationException e ) {
      throw new NamedClusterInitializationException( e );
    }
    return true;
  }
}
