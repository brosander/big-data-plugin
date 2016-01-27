package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceFactory;
import org.pentaho.bigdata.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;

/**
 * Created by bryan on 1/27/16.
 */
public class HBaseServiceFactory implements NamedClusterServiceFactory<HBaseService> {
  private final boolean isActiveConfiguration;
  private final HadoopConfiguration hadoopConfiguration;

  public HBaseServiceFactory( boolean isActiveConfiguration, HadoopConfiguration hadoopConfiguration ) {
    this.isActiveConfiguration = isActiveConfiguration;
    this.hadoopConfiguration = hadoopConfiguration;
  }

  @Override public Class<HBaseService> getServiceClass() {
    return HBaseService.class;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
    String shimIdentifier = null; // TODO: Specify shim
    return ( shimIdentifier == null && isActiveConfiguration ) || hadoopConfiguration.getIdentifier()
      .equals( shimIdentifier );
  }

  @Override public HBaseService create( NamedCluster namedCluster ) {
    try {
      return new HBaseServiceImpl( namedCluster, hadoopConfiguration );
    } catch ( ConfigurationException e ) {
      return null;
    }
  }
}
