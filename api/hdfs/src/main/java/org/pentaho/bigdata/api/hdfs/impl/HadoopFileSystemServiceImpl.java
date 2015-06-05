package org.pentaho.bigdata.api.hdfs.impl;

import org.pentaho.bigdata.api.configuration.NamedConfiguration;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemFactory;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemService;

import java.util.List;

/**
 * Created by bryan on 6/4/15.
 */
public class HadoopFileSystemServiceImpl implements HadoopFileSystemService {
  private final List<HadoopFileSystemFactory> hadoopFileSystemFactories;

  public HadoopFileSystemServiceImpl( List<HadoopFileSystemFactory> hadoopFileSystemFactories ) {
    this.hadoopFileSystemFactories = hadoopFileSystemFactories;
  }

  @Override public HadoopFileSystem getHadoopFilesystem( NamedConfiguration namedConfiguration ) {
    for ( HadoopFileSystemFactory hadoopFileSystemFactory : hadoopFileSystemFactories ) {
      if ( hadoopFileSystemFactory.canHandle( namedConfiguration ) ) {
        return hadoopFileSystemFactory.create( namedConfiguration );
      }
    }
    return null;
  }
}
