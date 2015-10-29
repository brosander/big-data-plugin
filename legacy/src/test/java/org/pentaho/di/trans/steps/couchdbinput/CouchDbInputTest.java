package org.pentaho.di.trans.steps.couchdbinput;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.trans.steps.mock.StepMockHelper;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/28/15.
 */
public class CouchDbInputTest {
  private String testName;
  private StepMockHelper stepMockHelper;
  private CouchDbInput couchDbInput;

  @Before
  public void setup() throws KettleException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init( false );
    Encr.init( "Kettle" );
    testName = "testName";
    stepMockHelper = new StepMockHelper( testName, CouchDbInputMeta.class, CouchDbInputData.class );
    when( stepMockHelper.logChannelInterfaceFactory.create( anyObject(), any( LoggingObjectInterface.class ) ) )
      .thenReturn( mock( LogChannelInterface.class ) );
    couchDbInput =
      new CouchDbInput( stepMockHelper.stepMeta, stepMockHelper.stepDataInterface, 0, stepMockHelper.transMeta,
        stepMockHelper.trans );
  }

  @Test
  public void testInit() throws IOException {
    CouchDbInputMeta couchDbInputMeta = (CouchDbInputMeta) stepMockHelper.initStepMetaInterface;
    CouchDbInputData couchDbInputData = (CouchDbInputData) stepMockHelper.initStepDataInterface;

    final String testHostname = "testHostname";
    final String testPort = "9999";
    final String testDbName = "testDbName";
    final String testDoc = "testDoc";
    final String testView = "testView";

    when( couchDbInputMeta.getHostname() ).thenReturn( testHostname );
    when( couchDbInputMeta.getPort() ).thenReturn( testPort );
    when( couchDbInputMeta.getDbName() ).thenReturn( testDbName );
    when( couchDbInputMeta.getDesignDocument() ).thenReturn( testDoc );
    when( couchDbInputMeta.getViewName() ).thenReturn( testView );

    CouchDbInput.HttpClientFactory httpClientFactory = mock( CouchDbInput.HttpClientFactory.class );
    CouchDbInput.GetMethodFactory getMethodFactory = mock( CouchDbInput.GetMethodFactory.class );

    GetMethod getMethod = mock( GetMethod.class );
    when( getMethodFactory.create( CouchDbInput
      .buildUrl( testHostname, Const.toInt( testPort, 5984 ), testDbName, testDoc, testView ) ) ).thenReturn(
      getMethod );

    HttpClient httpClient = mock( HttpClient.class );
    when( httpClientFactory.createHttpClient() ).thenReturn( httpClient );
    when( httpClient.executeMethod( getMethod ) ).thenReturn( 200 );
    couchDbInput =
      new CouchDbInput( stepMockHelper.stepMeta, stepMockHelper.stepDataInterface, 0, stepMockHelper.transMeta,
        stepMockHelper.trans, httpClientFactory, getMethodFactory );

    assertTrue( couchDbInput.init( couchDbInputMeta, couchDbInputData ) );
  }

  @Test
  public void testInitNoDesignDoc() throws IOException {
    CouchDbInputMeta couchDbInputMeta = (CouchDbInputMeta) stepMockHelper.initStepMetaInterface;
    CouchDbInputData couchDbInputData = (CouchDbInputData) stepMockHelper.initStepDataInterface;

    final String testHostname = "testHostname";
    final String testPort = "9999";
    final String testDbName = "testDbName";
    final String testDoc = "";
    final String testView = "testView";

    when( couchDbInputMeta.getHostname() ).thenReturn( testHostname );
    when( couchDbInputMeta.getPort() ).thenReturn( testPort );
    when( couchDbInputMeta.getDbName() ).thenReturn( testDbName );
    when( couchDbInputMeta.getDesignDocument() ).thenReturn( testDoc );
    when( couchDbInputMeta.getViewName() ).thenReturn( testView );

    CouchDbInput.HttpClientFactory httpClientFactory = mock( CouchDbInput.HttpClientFactory.class );
    CouchDbInput.GetMethodFactory getMethodFactory = mock( CouchDbInput.GetMethodFactory.class );

    GetMethod getMethod = mock( GetMethod.class );
    when( getMethodFactory.create( CouchDbInput
      .buildUrl( testHostname, Const.toInt( testPort, 5984 ), testDbName, testDoc, testView ) ) ).thenReturn(
      getMethod );

    HttpClient httpClient = mock( HttpClient.class );
    when( httpClientFactory.createHttpClient() ).thenReturn( httpClient );
    when( httpClient.executeMethod( getMethod ) ).thenReturn( 200 );
    couchDbInput =
      new CouchDbInput( stepMockHelper.stepMeta, stepMockHelper.stepDataInterface, 0, stepMockHelper.transMeta,
        stepMockHelper.trans, httpClientFactory, getMethodFactory );

    assertFalse( couchDbInput.init( couchDbInputMeta, couchDbInputData ) );
  }

  @Test
  public void testInitNoView() throws IOException {
    CouchDbInputMeta couchDbInputMeta = (CouchDbInputMeta) stepMockHelper.initStepMetaInterface;
    CouchDbInputData couchDbInputData = (CouchDbInputData) stepMockHelper.initStepDataInterface;

    final String testHostname = "testHostname";
    final String testPort = "9999";
    final String testDbName = "testDbName";
    final String testDoc = "testDoc";
    final String testView = "";

    when( couchDbInputMeta.getHostname() ).thenReturn( testHostname );
    when( couchDbInputMeta.getPort() ).thenReturn( testPort );
    when( couchDbInputMeta.getDbName() ).thenReturn( testDbName );
    when( couchDbInputMeta.getDesignDocument() ).thenReturn( testDoc );
    when( couchDbInputMeta.getViewName() ).thenReturn( testView );

    CouchDbInput.HttpClientFactory httpClientFactory = mock( CouchDbInput.HttpClientFactory.class );
    CouchDbInput.GetMethodFactory getMethodFactory = mock( CouchDbInput.GetMethodFactory.class );

    GetMethod getMethod = mock( GetMethod.class );
    when( getMethodFactory.create( CouchDbInput
      .buildUrl( testHostname, Const.toInt( testPort, 5984 ), testDbName, testDoc, testView ) ) ).thenReturn(
      getMethod );

    HttpClient httpClient = mock( HttpClient.class );
    when( httpClientFactory.createHttpClient() ).thenReturn( httpClient );
    when( httpClient.executeMethod( getMethod ) ).thenReturn( 200 );
    couchDbInput =
      new CouchDbInput( stepMockHelper.stepMeta, stepMockHelper.stepDataInterface, 0, stepMockHelper.transMeta,
        stepMockHelper.trans, httpClientFactory, getMethodFactory );

    assertFalse( couchDbInput.init( couchDbInputMeta, couchDbInputData ) );
  }
}
