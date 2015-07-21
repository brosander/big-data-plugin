package org.pentaho.bigdata.api.hdfs;

/**
 * Created by bryan on 5/27/15.
 */
public interface HadoopFileSystemPath {
  String getPath();
  String getName();
  String toUrl();

  HadoopFileSystemPath resolve( HadoopFileSystemPath child );
  HadoopFileSystemPath resolve( String child );

  HadoopFileSystemPath getParent();
}
