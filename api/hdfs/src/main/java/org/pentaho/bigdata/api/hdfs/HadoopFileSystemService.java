package org.pentaho.bigdata.api.hdfs;

import org.pentaho.bigdata.api.configuration.NamedConfiguration;

/**
 * Created by bryan on 5/22/15.
 */
public interface HadoopFileSystemService {
  HadoopFileSystem getHadoopFilesystem( NamedConfiguration namedConfiguration );
}