package com.pentaho.big.data.bundles.impl.shim.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.pentaho.bigdata.api.configuration.NamedConfiguration;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemFactory;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.io.IOException;

/**
 * Created by bryan on 5/28/15.
 */
public class HadoopFileSystemFactoryImpl implements HadoopFileSystemFactory {
  public static final String SHIM_IDENTIFIER = "shim.identifier";
  public static final String HDFS = "hdfs";
  private final boolean isActiveConfiguration;
  private final HadoopConfiguration hadoopConfiguration;

  public HadoopFileSystemFactoryImpl( boolean isActiveConfiguration, HadoopConfiguration hadoopConfiguration ) {
    this.isActiveConfiguration = isActiveConfiguration;
    this.hadoopConfiguration = hadoopConfiguration;
  }

  @Override public boolean canHandle( NamedConfiguration namedConfiguration ) {
    if ( !namedConfiguration.getConfigurationNamespaces().contains( HDFS ) ) {
      return false;
    }
    String shimIdentifier = namedConfiguration.getProperty( SHIM_IDENTIFIER );
    return ( shimIdentifier == null && isActiveConfiguration ) || hadoopConfiguration.getIdentifier()
      .equals( shimIdentifier );
  }

  @Override public HadoopFileSystem create( NamedConfiguration namedConfiguration ) {
    try {
      HadoopShim hadoopShim = hadoopConfiguration.getHadoopShim();
      return new HadoopFileSystemImpl(
        (FileSystem) hadoopShim.getFileSystem( hadoopShim.createConfiguration() ).getDelegate() );
    } catch ( IOException e ) {
      e.printStackTrace();
    }
    return null;
  }
}
