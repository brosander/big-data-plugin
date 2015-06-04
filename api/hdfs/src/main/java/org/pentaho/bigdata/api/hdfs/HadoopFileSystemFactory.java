package org.pentaho.bigdata.api.hdfs;

import org.pentaho.bigdata.api.configuration.NamedConfiguration;

/**
 * Created by bryan on 5/28/15.
 */
public interface HadoopFileSystemFactory {
  boolean canHandle( NamedConfiguration namedConfiguration );

  HadoopFileSystem create( NamedConfiguration namedConfiguration );
}
