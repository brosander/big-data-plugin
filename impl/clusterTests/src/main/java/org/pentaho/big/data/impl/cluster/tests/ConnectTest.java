package org.pentaho.big.data.impl.cluster.tests;

import org.pentaho.big.data.api.clusterTest.test.ClusterTest;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResult;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultEntryImpl;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultImpl;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bryan on 8/14/15.
 */
public class ConnectTest {
  private static final Class<?> PKG = ConnectTest.class;
  private final ClusterTest clusterTest;
  private final String hostname;
  private final String port;
  private final boolean haPossible;
  private final ClusterTestEntrySeverity severityOfFalures;

  public ConnectTest( ClusterTest clusterTest, String hostname, String port, boolean haPossible ) {
    this( clusterTest, hostname, port, haPossible, ClusterTestEntrySeverity.FATAL );
  }

  public ConnectTest( ClusterTest clusterTest, String hostname, String port, boolean haPossible,
                      ClusterTestEntrySeverity severityOfFailures ) {
    this.clusterTest = clusterTest;
    this.hostname = hostname;
    this.port = port;
    this.haPossible = haPossible;
    this.severityOfFalures = severityOfFailures;
  }

  public List<ClusterTestResultEntry> runTest() {
    List<ClusterTestResultEntry> clusterTestResultEntries = new ArrayList<>();
    if ( Const.isEmpty( hostname ) ) {
      clusterTestResultEntries.add( new ClusterTestResultEntryImpl( severityOfFalures,
        BaseMessages.getString( PKG, "ConnectTest.HostBlank.Desc" ),
        BaseMessages.getString( PKG, "ConnectTest.HostBlank.Message" ) ) );
    } else if ( Const.isEmpty( port ) ) {
      if ( haPossible ) {
        clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.INFO,
          BaseMessages.getString( PKG, "ConnectTest.HA.Desc" ),
          BaseMessages.getString( PKG, "ConnectTest.HA.Message" ) ) );
      } else {
        clusterTestResultEntries.add( new ClusterTestResultEntryImpl( severityOfFalures,
          BaseMessages.getString( PKG, "ConnectTest.PortBlank.Desc" ),
          BaseMessages.getString( PKG, "ConnectTest.PortBlank.Message" ) ) );
      }
    } else {
      Socket socket = null;
      try {
        if ( InetAddress.getByName( hostname ).isReachable( 10 * 1000 ) ) {
          try {
            socket = new Socket( hostname, Integer.valueOf( port ) );
            clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.INFO,
              BaseMessages.getString( PKG, "ConnectTest.ConnectSuccess.Desc" ),
              BaseMessages.getString( PKG, "ConnectTest.ConnectSuccess.Message", hostname, port ) ) );
          } catch ( IOException e ) {
            clusterTestResultEntries.add( new ClusterTestResultEntryImpl( severityOfFalures,
              BaseMessages.getString( PKG, "ConnectTest.ConnectFail.Desc" ),
              BaseMessages.getString( PKG, "ConnectTest.ConnectFail.Message", hostname, port ), e ) );
          } finally {
            if ( socket != null ) {
              try {
                socket.close();
              } catch ( IOException e ) {
                // Ignore
              }
            }
          }
        }
      } catch ( UnknownHostException e ) {
        clusterTestResultEntries.add( new ClusterTestResultEntryImpl( severityOfFalures,
          BaseMessages.getString( PKG, "ConnectTest.UnknownHostname.Desc" ),
          BaseMessages.getString( PKG, "ConnectTest.UnknownHostname.Message", hostname ), e ) );
      } catch ( IOException e ) {
        clusterTestResultEntries.add( new ClusterTestResultEntryImpl( severityOfFalures,
          BaseMessages.getString( PKG, "ConnectTest.NetworkError.Desc" ),
          BaseMessages.getString( PKG, "ConnectTest.NetworkError.Message" ), e ) );
      }
    }
    return clusterTestResultEntries;
  }
}
