package com.pentaho.big.data.bundles.impl.shim.hbase;

import com.pentaho.big.data.bundles.impl.shim.hbase.mapping.ColumnFilterFactoryImpl;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.hbase.ByteConversionUtil;
import org.pentaho.bigdata.api.hbase.HBaseConnection;
import org.pentaho.bigdata.api.hbase.HBaseService;
import org.pentaho.bigdata.api.hbase.mapping.ColumnFilterFactory;
import org.pentaho.bigdata.api.hbase.mapping.MappingFactory;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterfaceFactory;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hbase.shim.spi.HBaseShim;

import java.util.Properties;

/**
 * Created by bryan on 1/21/16.
 */
public class HBaseServiceImpl implements HBaseService {
  private final NamedCluster namedCluster;
  private final HBaseShim hBaseShim;

  public HBaseServiceImpl( NamedCluster namedCluster, HadoopConfiguration hadoopConfiguration )
    throws ConfigurationException {
    this.namedCluster = namedCluster;
    this.hBaseShim = hadoopConfiguration.getHBaseShim();
  }

  @Override public HBaseConnection getHBaseConnection( VariableSpace variableSpace, String siteConfig, String defaultConfig, LogChannelInterface logChannelInterface ) {
    Properties connProps = new Properties();
    String zooKeeperHost = variableSpace.environmentSubstitute( namedCluster.getZooKeeperHost() );
    String zooKeeperPort = variableSpace.environmentSubstitute( namedCluster.getZooKeeperPort() );
    if ( !Const.isEmpty( zooKeeperHost ) ) {
      connProps.setProperty( org.pentaho.hbase.shim.spi.HBaseConnection.ZOOKEEPER_QUORUM_KEY, zooKeeperHost );
    }
    if ( !Const.isEmpty( zooKeeperPort ) ) {
      connProps.setProperty( org.pentaho.hbase.shim.spi.HBaseConnection.ZOOKEEPER_PORT_KEY, zooKeeperPort );
    }
    if ( !Const.isEmpty( siteConfig ) ) {
      connProps.setProperty( org.pentaho.hbase.shim.spi.HBaseConnection.SITE_KEY, siteConfig );
    }
    if ( !Const.isEmpty( defaultConfig ) ) {
      connProps.setProperty( org.pentaho.hbase.shim.spi.HBaseConnection.DEFAULTS_KEY, defaultConfig );
    }
    return new HBaseConnectionImpl( this, hBaseShim, connProps, logChannelInterface );
  }

  @Override public ColumnFilterFactory getColumnFilterFactory() {
    return new ColumnFilterFactoryImpl();
  }

  @Override public MappingFactory getMappingFactory() {
    return null;
  }

  @Override public HBaseValueMetaInterfaceFactory getHBaseValueMetaInterfaceFactory() {
    return null;
  }

  @Override public ByteConversionUtil getByteConversionUtil() {
    return null;
  }
}
