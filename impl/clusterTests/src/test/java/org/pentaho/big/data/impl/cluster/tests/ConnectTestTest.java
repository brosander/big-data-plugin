package org.pentaho.big.data.impl.cluster.tests;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.clusterTest.TestMessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetter;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.big.data.api.clusterTest.ClusterTestEntryUtil.expectOneEntry;
import static org.pentaho.big.data.api.clusterTest.ClusterTestEntryUtil.verifyClusterTestResultEntry;

/**
 * Created by bryan on 8/21/15.
 */
public class ConnectTestTest {
  private String hostname;
  private String port;
  private boolean haPossible;
  private ClusterTestEntrySeverity severityOfFailures;
  private ConnectTest.SocketFactory socketFactory;
  private ConnectTest.InetAddressFactory inetAddressFactory;
  private ConnectTest connectTest;
  private MessageGetterFactory messageGetterFactory;
  private MessageGetter messageGetter;
  private InetAddress inetAddress;
  private Socket socket;

  @Before
  public void setup() throws IOException {
    messageGetterFactory = new TestMessageGetterFactory();
    messageGetter = messageGetterFactory.create( ConnectTest.class );
    hostname = "hostname";
    port = "89";
    haPossible = false;
    severityOfFailures = ClusterTestEntrySeverity.WARNING;
    socketFactory = mock( ConnectTest.SocketFactory.class );
    socket = mock( Socket.class );
    when( socketFactory.create( hostname, Integer.valueOf( port ) ) ).thenReturn( socket );
    inetAddressFactory = mock( ConnectTest.InetAddressFactory.class );
    inetAddress = mock( InetAddress.class );
    when( inetAddressFactory.create( hostname ) ).thenReturn( inetAddress );
    when( inetAddress.isReachable( anyInt() ) ).thenReturn( true );
    init();
  }

  private void init() {
    connectTest =
      new ConnectTest( messageGetterFactory, hostname, port, haPossible, severityOfFailures, socketFactory,
        inetAddressFactory );
  }

  @Test
  public void testBlankHostname() {
    hostname = "";
    init();
    verifyClusterTestResultEntry( expectOneEntry( connectTest.runTest() ), severityOfFailures,
      messageGetter.getMessage( ConnectTest.CONNECT_TEST_HOST_BLANK_DESC ), messageGetter.getMessage(
        ConnectTest.CONNECT_TEST_HOST_BLANK_MESSAGE ) );
  }

  @Test
  public void testBlankPortNoHa() {
    port = "";
    init();
    verifyClusterTestResultEntry( expectOneEntry( connectTest.runTest() ), severityOfFailures,
      messageGetter.getMessage( ConnectTest.CONNECT_TEST_PORT_BLANK_DESC ), messageGetter.getMessage(
        ConnectTest.CONNECT_TEST_PORT_BLANK_MESSAGE ) );
  }

  @Test
  public void testBlankPortHa() {
    port = "";
    haPossible = true;
    init();
    verifyClusterTestResultEntry( expectOneEntry( connectTest.runTest() ), ClusterTestEntrySeverity.INFO,
      messageGetter.getMessage( ConnectTest.CONNECT_TEST_HA_DESC ), messageGetter.getMessage(
        ConnectTest.CONNECT_TEST_HA_MESSAGE ) );
  }

  @Test
  public void testNonNumericPort() {
    port = "abc";
    haPossible = true;
    init();
    verifyClusterTestResultEntry( expectOneEntry( connectTest.runTest() ), ClusterTestEntrySeverity.FATAL,
      messageGetter.getMessage( ConnectTest.CONNECT_TEST_PORT_NUMBER_FORMAT_DESC ), messageGetter.getMessage(
        ConnectTest.CONNECT_TEST_PORT_NUMBER_FORMAT_MESSAGE ), NumberFormatException.class );
  }

  @Test
  public void testUnreachableHostname() throws IOException {
    inetAddressFactory = mock( ConnectTest.InetAddressFactory.class );
    inetAddress = mock( InetAddress.class );
    when( inetAddressFactory.create( hostname ) ).thenReturn( inetAddress );
    when( inetAddress.isReachable( anyInt() ) ).thenReturn( false );
    init();
    verifyClusterTestResultEntry( expectOneEntry( connectTest.runTest() ), severityOfFailures,
      messageGetter.getMessage( ConnectTest.CONNECT_TEST_UNREACHABLE_DESC ), messageGetter.getMessage(
        ConnectTest.CONNECT_TEST_UNREACHABLE_MESSAGE, hostname ) );
  }

  @Test
  public void testUnknownHostException() throws IOException {
    inetAddressFactory = mock( ConnectTest.InetAddressFactory.class );
    inetAddress = mock( InetAddress.class );
    when( inetAddressFactory.create( hostname ) ).thenReturn( inetAddress );
    when( inetAddress.isReachable( anyInt() ) ).thenThrow( new UnknownHostException() );
    init();
    verifyClusterTestResultEntry( expectOneEntry( connectTest.runTest() ), severityOfFailures,
      messageGetter.getMessage( ConnectTest.CONNECT_TEST_UNKNOWN_HOSTNAME_DESC ), messageGetter.getMessage(
        ConnectTest.CONNECT_TEST_UNKNOWN_HOSTNAME_MESSAGE, hostname ), UnknownHostException.class );
  }

  @Test
  public void testReachableIOException() throws IOException {
    inetAddressFactory = mock( ConnectTest.InetAddressFactory.class );
    inetAddress = mock( InetAddress.class );
    when( inetAddressFactory.create( hostname ) ).thenReturn( inetAddress );
    when( inetAddress.isReachable( anyInt() ) ).thenThrow( new IOException() );
    init();
    verifyClusterTestResultEntry( expectOneEntry( connectTest.runTest() ), severityOfFailures,
      messageGetter.getMessage( ConnectTest.CONNECT_TEST_NETWORK_ERROR_DESC ), messageGetter.getMessage(
        ConnectTest.CONNECT_TEST_NETWORK_ERROR_MESSAGE, hostname, port ), IOException.class );
  }

  @Test
  public void testSocketIOException() throws IOException {
    when( socketFactory.create( hostname, Integer.valueOf( port ) ) ).thenThrow( new IOException() );
    init();
    verifyClusterTestResultEntry( expectOneEntry( connectTest.runTest() ), severityOfFailures,
      messageGetter.getMessage( ConnectTest.CONNECT_TEST_CONNECT_FAIL_DESC ), messageGetter.getMessage(
        ConnectTest.CONNECT_TEST_CONNECT_FAIL_MESSAGE, hostname, port ), IOException.class );
  }

  @Test
  public void testSuccess() throws IOException {
    verifyClusterTestResultEntry( expectOneEntry( connectTest.runTest() ), ClusterTestEntrySeverity.INFO,
      messageGetter.getMessage( ConnectTest.CONNECT_TEST_CONNECT_SUCCESS_DESC ), messageGetter.getMessage(
        ConnectTest.CONNECT_TEST_CONNECT_SUCCESS_MESSAGE, hostname, port ) );
    verify( socket ).close();
  }
}
