package org.pentaho.big.data.impl.cluster.tests;

import org.pentaho.big.data.api.clusterTest.i18n.MessageGetter;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultEntryImpl;
import org.pentaho.di.core.Const;

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
  public static final String CONNECT_TEST_HOST_BLANK_DESC = "ConnectTest.HostBlank.Desc";
  public static final String CONNECT_TEST_HOST_BLANK_MESSAGE = "ConnectTest.HostBlank.Message";
  public static final String CONNECT_TEST_HA_DESC = "ConnectTest.HA.Desc";
  public static final String CONNECT_TEST_HA_MESSAGE = "ConnectTest.HA.Message";
  public static final String CONNECT_TEST_PORT_BLANK_DESC = "ConnectTest.PortBlank.Desc";
  public static final String CONNECT_TEST_PORT_BLANK_MESSAGE = "ConnectTest.PortBlank.Message";
  public static final String CONNECT_TEST_CONNECT_SUCCESS_DESC = "ConnectTest.ConnectSuccess.Desc";
  public static final String CONNECT_TEST_CONNECT_SUCCESS_MESSAGE = "ConnectTest.ConnectSuccess.Message";
  public static final String CONNECT_TEST_CONNECT_FAIL_DESC = "ConnectTest.ConnectFail.Desc";
  public static final String CONNECT_TEST_CONNECT_FAIL_MESSAGE = "ConnectTest.ConnectFail.Message";
  public static final String CONNECT_TEST_UNKNOWN_HOSTNAME_DESC = "ConnectTest.UnknownHostname.Desc";
  public static final String CONNECT_TEST_UNKNOWN_HOSTNAME_MESSAGE = "ConnectTest.UnknownHostname.Message";
  public static final String CONNECT_TEST_NETWORK_ERROR_DESC = "ConnectTest.NetworkError.Desc";
  public static final String CONNECT_TEST_NETWORK_ERROR_MESSAGE = "ConnectTest.NetworkError.Message";
  public static final String CONNECT_TEST_PORT_NUMBER_FORMAT_DESC = "ConnectTest.PortNumberFormat.Desc";
  public static final String CONNECT_TEST_PORT_NUMBER_FORMAT_MESSAGE = "ConnectTest.PortNumberFormat.Message";
  public static final String CONNECT_TEST_UNREACHABLE_DESC = "ConnectTest.Unreachable.Desc";
  public static final String CONNECT_TEST_UNREACHABLE_MESSAGE = "ConnectTest.Unreachable.Message";
  private static final Class<?> PKG = ConnectTest.class;
  private final MessageGetter messageGetter;
  private final String hostname;
  private final String port;
  private final boolean haPossible;
  private final ClusterTestEntrySeverity severityOfFalures;
  private final SocketFactory socketFactory;
  private final InetAddressFactory inetAddressFactory;

  public ConnectTest( MessageGetterFactory messageGetterFactory, String hostname, String port, boolean haPossible ) {
    this( messageGetterFactory, hostname, port, haPossible, ClusterTestEntrySeverity.FATAL );
  }

  public ConnectTest( MessageGetterFactory messageGetterFactory, String hostname, String port, boolean haPossible,
                      ClusterTestEntrySeverity severityOfFailures ) {
    this( messageGetterFactory, hostname, port, haPossible, severityOfFailures, new SocketFactory(),
      new InetAddressFactory() );
  }

  public ConnectTest( MessageGetterFactory messageGetterFactory, String hostname, String port, boolean haPossible,
                      ClusterTestEntrySeverity severityOfFailures, SocketFactory socketFactory,
                      InetAddressFactory inetAddressFactory ) {
    this.messageGetter = messageGetterFactory.create( PKG );
    this.hostname = hostname;
    this.port = port;
    this.haPossible = haPossible;
    this.severityOfFalures = severityOfFailures;
    this.socketFactory = socketFactory;
    this.inetAddressFactory = inetAddressFactory;
  }

  public List<ClusterTestResultEntry> runTest() {
    List<ClusterTestResultEntry> clusterTestResultEntries = new ArrayList<>();
    if ( Const.isEmpty( hostname ) ) {
      clusterTestResultEntries.add( new ClusterTestResultEntryImpl( severityOfFalures,
        messageGetter.getMessage( CONNECT_TEST_HOST_BLANK_DESC ),
        messageGetter.getMessage( CONNECT_TEST_HOST_BLANK_MESSAGE ) ) );
    } else if ( Const.isEmpty( port ) ) {
      if ( haPossible ) {
        clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.INFO,
          messageGetter.getMessage( CONNECT_TEST_HA_DESC ),
          messageGetter.getMessage( CONNECT_TEST_HA_MESSAGE ) ) );
      } else {
        clusterTestResultEntries.add( new ClusterTestResultEntryImpl( severityOfFalures,
          messageGetter.getMessage( CONNECT_TEST_PORT_BLANK_DESC ),
          messageGetter.getMessage( CONNECT_TEST_PORT_BLANK_MESSAGE ) ) );
      }
    } else {
      Socket socket = null;
      try {
        if ( inetAddressFactory.create( hostname ).isReachable( 10 * 1000 ) ) {
          try {
            socket = socketFactory.create( hostname, Integer.valueOf( port ) );
            clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.INFO,
              messageGetter.getMessage( CONNECT_TEST_CONNECT_SUCCESS_DESC ),
              messageGetter.getMessage( CONNECT_TEST_CONNECT_SUCCESS_MESSAGE, hostname, port ) ) );
          } catch ( IOException e ) {
            clusterTestResultEntries.add( new ClusterTestResultEntryImpl( severityOfFalures,
              messageGetter.getMessage( CONNECT_TEST_CONNECT_FAIL_DESC ),
              messageGetter.getMessage( CONNECT_TEST_CONNECT_FAIL_MESSAGE, hostname, port ), e ) );
          } finally {
            if ( socket != null ) {
              try {
                socket.close();
              } catch ( IOException e ) {
                // Ignore
              }
            }
          }
        } else {
          clusterTestResultEntries.add( new ClusterTestResultEntryImpl( severityOfFalures,
            messageGetter.getMessage( CONNECT_TEST_UNREACHABLE_DESC ),
            messageGetter.getMessage( CONNECT_TEST_UNREACHABLE_MESSAGE, hostname ) ) );
        }
      } catch ( UnknownHostException e ) {
        clusterTestResultEntries.add( new ClusterTestResultEntryImpl( severityOfFalures,
          messageGetter.getMessage( CONNECT_TEST_UNKNOWN_HOSTNAME_DESC ),
          messageGetter.getMessage( CONNECT_TEST_UNKNOWN_HOSTNAME_MESSAGE, hostname ), e ) );
      } catch ( IOException e ) {
        clusterTestResultEntries.add( new ClusterTestResultEntryImpl( severityOfFalures,
          messageGetter.getMessage( CONNECT_TEST_NETWORK_ERROR_DESC ),
          messageGetter.getMessage( CONNECT_TEST_NETWORK_ERROR_MESSAGE, hostname, port ), e ) );
      } catch ( NumberFormatException e ) {
        clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
          messageGetter.getMessage( CONNECT_TEST_PORT_NUMBER_FORMAT_DESC ),
          messageGetter.getMessage( CONNECT_TEST_PORT_NUMBER_FORMAT_MESSAGE ), e ) );
      }
    }
    return clusterTestResultEntries;
  }

  /**
   * Pulled out class to enable mock injection in tests
   */
  public static class SocketFactory {
    public Socket create( String hostname, int port ) throws IOException {
      return new Socket( hostname, port );
    }
  }

  /**
   * Pulled out class to enable mock injection in tests
   */
  public static class InetAddressFactory {
    public InetAddress create( String hostname ) throws UnknownHostException {
      return InetAddress.getByName( hostname );
    }
  }
}
