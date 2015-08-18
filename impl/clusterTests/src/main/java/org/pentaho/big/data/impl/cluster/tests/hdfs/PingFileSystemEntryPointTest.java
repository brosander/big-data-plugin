package org.pentaho.big.data.impl.cluster.tests.hdfs;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResult;
import org.pentaho.big.data.api.clusterTest.test.impl.BaseClusterTest;
import org.pentaho.big.data.impl.cluster.tests.ConnectTest;
import org.pentaho.big.data.impl.cluster.tests.Constants;
import org.pentaho.di.i18n.BaseMessages;

import java.util.HashSet;

/**
 * Created by bryan on 8/14/15.
 */
public class PingFileSystemEntryPointTest extends BaseClusterTest {
  public static final String HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST =
    "hadoopFileSystemPingFileSystemEntryPointTest";
  private static final Class<?> PKG = PingFileSystemEntryPointTest.class;

  public PingFileSystemEntryPointTest() {
    super( Constants.HADOOP_FILE_SYSTEM, HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST,
      BaseMessages.getString( PKG, "PingFileSystemEntryPointTest.Name" ),
      new HashSet<String>() );
  }

  @Override public ClusterTestResult runTest( NamedCluster namedCluster ) {
    return new ConnectTest( this, namedCluster.getHdfsHost(), namedCluster.getHdfsPort(), true ).runTest();
  }
}
