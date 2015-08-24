package org.pentaho.big.data.impl.cluster.tests.hdfs;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.TestMessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetter;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.network.ConnectivityTest;
import org.pentaho.big.data.api.clusterTest.network.ConnectivityTestFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/24/15.
 */
public class PingFileSystemEntryPointTestTest {
  private MessageGetterFactory messageGetterFactory;
  private ConnectivityTestFactory connectivityTestFactory;
  private PingFileSystemEntryPointTest fileSystemEntryPointTest;
  private NamedCluster namedCluster;
  private MessageGetter messageGetter;
  private String hdfsHost;
  private String hdfsPort;

  @Before
  public void setup() {
    messageGetterFactory = new TestMessageGetterFactory();
    messageGetter = messageGetterFactory.create( PingFileSystemEntryPointTest.class );
    connectivityTestFactory = mock( ConnectivityTestFactory.class );
    fileSystemEntryPointTest = new PingFileSystemEntryPointTest( messageGetterFactory, connectivityTestFactory );
    hdfsHost = "hdfsHost";
    hdfsPort = "8025";
    namedCluster = mock( NamedCluster.class );
    when( namedCluster.getHdfsHost() ).thenReturn( hdfsHost );
    when( namedCluster.getHdfsPort() ).thenReturn( hdfsPort );
  }

  @Test
  public void testGetName() {
    assertEquals( messageGetter.getMessage( PingFileSystemEntryPointTest.PING_FILE_SYSTEM_ENTRY_POINT_TEST_NAME ),
      fileSystemEntryPointTest.getName() );
  }

  @Test
  public void testSuccess() {
    List<ClusterTestResultEntry> results = mock( List.class );
    ConnectivityTest connectivityTest = mock( ConnectivityTest.class );
    when( connectivityTestFactory.create( messageGetterFactory, hdfsHost, hdfsPort, true ) )
      .thenReturn( connectivityTest );
    when( connectivityTest.runTest() ).thenReturn( results );
    assertEquals( results, fileSystemEntryPointTest.runTest( namedCluster ) );
  }
}
