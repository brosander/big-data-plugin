package org.pentaho.big.data.impl.cluster.tests.mr;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.network.ConnectivityTestFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.BaseClusterTest;
import org.pentaho.big.data.impl.cluster.tests.Constants;

import java.util.HashSet;
import java.util.List;

/**
 * Created by bryan on 8/14/15.
 */
public class PingJobTrackerTest extends BaseClusterTest {
  public static final String JOB_TRACKER_PING_JOB_TRACKER_TEST =
    "jobTrackerPingJobTrackerTest";
  public static final String PING_JOB_TRACKER_TEST_NAME = "PingJobTrackerTest.Name";
  private static final Class<?> PKG = PingJobTrackerTest.class;
  private final MessageGetterFactory messageGetterFactory;
  private final ConnectivityTestFactory connectivityTestFactory;

  public PingJobTrackerTest( MessageGetterFactory messageGetterFactory,
                             ConnectivityTestFactory connectivityTestFactory ) {
    super( Constants.MAP_REDUCE, JOB_TRACKER_PING_JOB_TRACKER_TEST,
      messageGetterFactory.create( PKG ).getMessage( PING_JOB_TRACKER_TEST_NAME ), new HashSet<String>() );
    this.messageGetterFactory = messageGetterFactory;
    this.connectivityTestFactory = connectivityTestFactory;
  }

  @Override public List<ClusterTestResultEntry> runTest( NamedCluster namedCluster ) {
    return connectivityTestFactory
      .create( messageGetterFactory, namedCluster.getJobTrackerHost(), namedCluster.getJobTrackerPort(), true )
      .runTest();
  }
}
