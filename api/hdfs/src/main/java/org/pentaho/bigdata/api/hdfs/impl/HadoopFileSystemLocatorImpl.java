package org.pentaho.bigdata.api.hdfs.impl;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemFactory;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;

import java.util.List;

/**
 * Created by bryan on 6/4/15.
 */
public class HadoopFileSystemLocatorImpl implements HadoopFileSystemLocator {
  private final List<HadoopFileSystemFactory> hadoopFileSystemFactories;

  public HadoopFileSystemLocatorImpl( List<HadoopFileSystemFactory> hadoopFileSystemFactories ) {
    this.hadoopFileSystemFactories = hadoopFileSystemFactories;
  }

  @Override public HadoopFileSystem getHadoopFilesystem( NamedCluster namedCluster ) {
    for ( HadoopFileSystemFactory hadoopFileSystemFactory : hadoopFileSystemFactories ) {
      if ( hadoopFileSystemFactory.canHandle( namedCluster ) ) {
        return hadoopFileSystemFactory.create( namedCluster );
      }
    }
    return null;
  }
}
