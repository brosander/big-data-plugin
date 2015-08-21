package org.pentaho.big.data.impl.cluster.tests.mr;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.BaseClusterTest;
import org.pentaho.big.data.impl.cluster.tests.ConnectTest;
import org.pentaho.big.data.impl.cluster.tests.Constants;
import org.pentaho.di.i18n.BaseMessages;

import java.util.HashSet;
import java.util.List;

/**
 * Created by bryan on 8/14/15.
 */
public class PingJobTrackerTest extends BaseClusterTest {
  public static final String JOB_TRACKER_PING_JOB_TRACKER_TEST =
    "jobTrackerPingJobTrackerTest";
  private static final Class<?> PKG = PingJobTrackerTest.class;
  private final MessageGetterFactory messageGetterFactory;

  public PingJobTrackerTest( MessageGetterFactory messageGetterFactory ) {
    super( Constants.MAP_REDUCE, JOB_TRACKER_PING_JOB_TRACKER_TEST,
      BaseMessages.getString( PKG, "PingJobTrackerTest.Name" ), new HashSet<String>() );
    this.messageGetterFactory = messageGetterFactory;
  }

  @Override public List<ClusterTestResultEntry> runTest( NamedCluster namedCluster ) {
    return new ConnectTest( messageGetterFactory, namedCluster.getJobTrackerHost(), namedCluster.getJobTrackerPort(),
      true ).runTest();
  }
}
