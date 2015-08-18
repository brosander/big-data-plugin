package org.pentaho.big.data.impl.cluster.tests.zookeeper;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResult;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.BaseClusterTest;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultEntryImpl;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultImpl;
import org.pentaho.big.data.impl.cluster.tests.ConnectTest;
import org.pentaho.big.data.impl.cluster.tests.Constants;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Created by bryan on 8/14/15.
 */
public class PingZookeeperEnsembleTest extends BaseClusterTest {
  public static final String HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST =
    "zookeeperPingZookeeperEnsembleTest";
  private static final Class<?> PKG = PingZookeeperEnsembleTest.class;

  public PingZookeeperEnsembleTest() {
    super( Constants.ZOOKEEPER, HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST,
      BaseMessages.getString( PKG, "PingZookeeperEnsembleTest.Name" ), new HashSet<String>() );
  }

  @Override public ClusterTestResult runTest( NamedCluster namedCluster ) {
    String zooKeeperHost = namedCluster.getZooKeeperHost();
    String zooKeeperPort = namedCluster.getZooKeeperPort();
    if ( Const.isEmpty( zooKeeperHost ) ) {
      return new ClusterTestResultImpl( this, new ArrayList<ClusterTestResultEntry>( Arrays.asList(
        new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
          BaseMessages.getString( PKG, "PingZookeeperEnsembleTest.BlankHost" ),
          BaseMessages.getString( PKG, "PingZookeeperEnsembleTest.BlankHost" ) ) ) ) );
    } else if ( Const.isEmpty( zooKeeperPort ) ) {
      return new ClusterTestResultImpl( this, new ArrayList<ClusterTestResultEntry>( Arrays.asList(
        new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
          BaseMessages.getString( PKG, "PingZookeeperEnsembleTest.BlankPort" ),
          BaseMessages.getString( PKG, "PingZookeeperEnsembleTest.BlankPort" ) ) ) ) );
    } else {
      String[] quorum = namedCluster.getZooKeeperHost().split( "," );
      List<ClusterTestResultEntry> clusterTestResultEntries = new ArrayList<>();
      boolean hadSuccess = false;
      for ( String node : quorum ) {
        ClusterTestResult nodeResults =
          new ConnectTest( this, node, zooKeeperPort, false, ClusterTestEntrySeverity.WARNING ).runTest();
        if ( nodeResults.getMaxSeverity() != ClusterTestEntrySeverity.WARNING ) {
          hadSuccess = true;
        }
        clusterTestResultEntries.addAll( nodeResults.getClusterTestResultEntries() );
      }
      if ( !hadSuccess ) {
        List<ClusterTestResultEntry> newClusterTestResultEntries =
          new ArrayList<>( clusterTestResultEntries.size() + 1 );
        newClusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
          BaseMessages.getString( PKG, "PingOozieHostTest.NoNodesSucceeded.Desc" ),
          BaseMessages.getString( PKG, "PingOozieHostTest.NoNodesSucceeded.Message" ) ) );
        newClusterTestResultEntries.addAll( clusterTestResultEntries );
        clusterTestResultEntries = newClusterTestResultEntries;
      }
      return new ClusterTestResultImpl( this, clusterTestResultEntries );
    }
  }
}
