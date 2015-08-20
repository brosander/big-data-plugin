package org.pentaho.big.data.impl.cluster.tests.oozie;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.BaseClusterTest;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultEntryImpl;
import org.pentaho.big.data.impl.cluster.tests.ConnectTest;
import org.pentaho.big.data.impl.cluster.tests.Constants;
import org.pentaho.di.i18n.BaseMessages;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Created by bryan on 8/14/15.
 */
public class PingOozieHostTest extends BaseClusterTest {
  public static final String OOZIE_PING_OOZIE_HOST_TEST =
    "ooziePingOozieHostTest";
  private static final Class<?> PKG = PingOozieHostTest.class;

  public PingOozieHostTest() {
    super( Constants.OOZIE, OOZIE_PING_OOZIE_HOST_TEST, BaseMessages.getString( PKG, "PingOozieHostTest.Name" ),
      new HashSet<String>() );
  }

  @Override public List<ClusterTestResultEntry> runTest( NamedCluster namedCluster ) {
    String oozieUrl = namedCluster.getOozieUrl();
    try {
      URL url = new URL( oozieUrl );
      return new ConnectTest( this, url.getHost(), String.valueOf( url.getPort() ), false ).runTest();
    } catch ( MalformedURLException e ) {
      return new ArrayList<ClusterTestResultEntry>( Arrays.asList(
        new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
          BaseMessages.getString( PKG, "PingOozieHostTest.MalformedUrlDesc" ),
          BaseMessages.getString( PKG, "PingOozieHostTest.MalformedUrlDesc" ), e ) ) );
    }
  }
}
