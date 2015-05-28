package org.pentaho.bigdata.api.hdfs;

/**
 * Created by bryan on 5/22/15.
 */
public interface HadoopFileSystemService {
    HadoopFileSystem getHadoopFilesystem(String uri);
}