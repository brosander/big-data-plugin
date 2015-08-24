package org.pentaho.big.data.impl.shim.tests;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.TestMessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetter;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.hadoop.NoShimSpecifiedException;
import org.pentaho.hadoop.shim.ConfigurationException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.pentaho.big.data.api.clusterTest.ClusterTestEntryUtil.expectOneEntry;
import static org.pentaho.big.data.api.clusterTest.ClusterTestEntryUtil.verifyClusterTestResultEntry;

/**
 * Created by bryan on 8/24/15.
 */
public class TestShimLoadTest {
  private MessageGetterFactory messageGetterFactory;
  private HadoopConfigurationBootstrap hadoopConfigurationBootstrap;
  private MessageGetter messageGetter;
  private TestShimLoad testShimLoad;
  private NamedCluster namedCluster;

  @Before
  public void setup() {
    messageGetterFactory = new TestMessageGetterFactory();
    messageGetter = messageGetterFactory.create( TestShimLoad.class );
    hadoopConfigurationBootstrap = mock( HadoopConfigurationBootstrap.class );
    testShimLoad = new TestShimLoad( messageGetterFactory, hadoopConfigurationBootstrap );
    namedCluster = mock( NamedCluster.class );
  }

  @Test
  public void testGetName() {
    assertEquals( messageGetter.getMessage( TestShimLoad.TEST_SHIM_LOAD_NAME ), testShimLoad.getName() );
  }

  @Test
  public void testConfigurationException() throws ConfigurationException {
    String testMessage = "testMessage";
    when( hadoopConfigurationBootstrap.getProvider() ).thenThrow( new ConfigurationException( testMessage ) );
    verifyClusterTestResultEntry( expectOneEntry( testShimLoad.runTest( namedCluster ) ),
      ClusterTestEntrySeverity.ERROR, messageGetter.getMessage( TestShimLoad.TEST_SHIM_LOAD_UNABLE_TO_LOAD_SHIM_DESC ),
      testMessage, ConfigurationException.class );
  }

  @Test
  public void testNoShimSpecified() throws ConfigurationException {
    String testMessage = "testMessage";
    when( hadoopConfigurationBootstrap.getProvider() ).thenThrow( new NoShimSpecifiedException( testMessage ) );
    verifyClusterTestResultEntry( expectOneEntry( testShimLoad.runTest( namedCluster ) ),
      ClusterTestEntrySeverity.ERROR, messageGetter.getMessage( TestShimLoad.TEST_SHIM_LOAD_NO_SHIM_SPECIFIED_DESC ),
      testMessage, NoShimSpecifiedException.class );
  }

  @Test
  public void testSuccess() throws ConfigurationException {
    String testShim = "testShim";
    when( hadoopConfigurationBootstrap.getActiveConfigurationId() ).thenReturn( testShim );
    verifyClusterTestResultEntry( expectOneEntry( testShimLoad.runTest( namedCluster ) ),
      ClusterTestEntrySeverity.INFO, messageGetter.getMessage( TestShimLoad.TEST_SHIM_LOAD_SHIM_LOADED_DESC, testShim ),
      messageGetter.getMessage( TestShimLoad.TEST_SHIM_LOAD_SHIM_LOADED_MESSAGE, testShim ) );
  }
}
