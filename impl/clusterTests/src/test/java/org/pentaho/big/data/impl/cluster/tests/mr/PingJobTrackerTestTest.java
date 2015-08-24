package org.pentaho.big.data.impl.cluster.tests.mr;

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
public class PingJobTrackerTestTest {
  private MessageGetterFactory messageGetterFactory;
  private ConnectivityTestFactory connectivityTestFactory;
  private PingJobTrackerTest pingJobTrackerTest;
  private NamedCluster namedCluster;
  private MessageGetter messageGetter;
  private String jobTrackerHost;
  private String jobTrackerPort;

  @Before
  public void setup() {
    messageGetterFactory = new TestMessageGetterFactory();
    messageGetter = messageGetterFactory.create( PingJobTrackerTest.class );
    connectivityTestFactory = mock( ConnectivityTestFactory.class );
    pingJobTrackerTest = new PingJobTrackerTest( messageGetterFactory, connectivityTestFactory );
    jobTrackerHost = "jobTrackerHost";
    jobTrackerPort = "829";
    namedCluster = mock( NamedCluster.class );
    when( namedCluster.getJobTrackerHost() ).thenReturn( jobTrackerHost );
    when( namedCluster.getJobTrackerPort() ).thenReturn( jobTrackerPort );
  }

  @Test
  public void testGetName() {
    assertEquals( messageGetter.getMessage( PingJobTrackerTest.PING_JOB_TRACKER_TEST_NAME ),
      pingJobTrackerTest.getName() );
  }

  @Test
  public void testSuccess() {
    List<ClusterTestResultEntry> results = mock( List.class );
    ConnectivityTest connectivityTest = mock( ConnectivityTest.class );
    when( connectivityTestFactory.create( messageGetterFactory, jobTrackerHost, jobTrackerPort, true ) )
      .thenReturn( connectivityTest );
    when( connectivityTest.runTest() ).thenReturn( results );
    assertEquals( results, pingJobTrackerTest.runTest( namedCluster ) );
  }
}
