package org.pentaho.big.data.impl.cluster.tests.zookeeper;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.BaseClusterTest;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultEntryImpl;
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
  private final MessageGetterFactory messageGetterFactory;

  public PingZookeeperEnsembleTest( MessageGetterFactory messageGetterFactory ) {
    super( Constants.ZOOKEEPER, HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST,
      BaseMessages.getString( PKG, "PingZookeeperEnsembleTest.Name" ), new HashSet<String>() );
    this.messageGetterFactory = messageGetterFactory;
  }

  @Override public List<ClusterTestResultEntry> runTest( NamedCluster namedCluster ) {
    String zooKeeperHost = namedCluster.getZooKeeperHost();
    String zooKeeperPort = namedCluster.getZooKeeperPort();
    if ( Const.isEmpty( zooKeeperHost ) ) {
      return new ArrayList<ClusterTestResultEntry>( Arrays.asList(
        new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
          BaseMessages.getString( PKG, "PingZookeeperEnsembleTest.BlankHost.Desc" ),
          BaseMessages.getString( PKG, "PingZookeeperEnsembleTest.BlankHost.Message" ) ) ) );
    } else if ( Const.isEmpty( zooKeeperPort ) ) {
      return new ArrayList<ClusterTestResultEntry>( Arrays.asList(
        new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
          BaseMessages.getString( PKG, "PingZookeeperEnsembleTest.BlankPort.Desc" ),
          BaseMessages.getString( PKG, "PingZookeeperEnsembleTest.BlankPort.Message" ) ) ) );
    } else {
      String[] quorum = namedCluster.getZooKeeperHost().split( "," );
      List<ClusterTestResultEntry> clusterTestResultEntries = new ArrayList<>();
      boolean hadSuccess = false;
      for ( String node : quorum ) {
        List<ClusterTestResultEntry> nodeResults =
          new ConnectTest( messageGetterFactory, node, zooKeeperPort, false, ClusterTestEntrySeverity.WARNING )
            .runTest();
        if ( ClusterTestEntrySeverity.maxSeverityEntry( nodeResults ) != ClusterTestEntrySeverity.WARNING ) {
          hadSuccess = true;
        }
        clusterTestResultEntries.addAll( nodeResults );
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
      return clusterTestResultEntries;
    }
  }
}
