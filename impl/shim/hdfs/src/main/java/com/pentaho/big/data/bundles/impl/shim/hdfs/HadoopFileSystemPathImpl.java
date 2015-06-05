package com.pentaho.big.data.bundles.impl.shim.hdfs;

import org.apache.hadoop.fs.Path;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemPath;

/**
 * Created by bryan on 5/28/15.
 */
public class HadoopFileSystemPathImpl implements HadoopFileSystemPath {
  private final Path path;

  public HadoopFileSystemPathImpl( Path path ) {
    this.path = path;
  }

  @Override
  public String getPath() {
    return path.toUri().getPath();
  }

  @Override public String getName() {
    return path.getName();
  }
}
