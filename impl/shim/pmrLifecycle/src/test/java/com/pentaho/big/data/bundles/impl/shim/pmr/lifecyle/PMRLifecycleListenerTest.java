package com.pentaho.big.data.bundles.impl.shim.pmr.lifecyle;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.shim.ConfigurationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 9/2/15.
 */
public class PMRLifecycleListenerTest {
  private HadoopConfigurationBootstrap hadoopConfigurationBootstrap;
  private PMRLifecycleListener pmrLifecycleListener;

  @Before
  public void setup() {
    hadoopConfigurationBootstrap = mock( HadoopConfigurationBootstrap.class );
    pmrLifecycleListener = new PMRLifecycleListener( hadoopConfigurationBootstrap );
  }

  @Test
  public void testNoArgConstructor() {
    assertEquals( HadoopConfigurationBootstrap.getInstance(),
      new PMRLifecycleListener().getHadoopConfigurationBootstrap() );
  }

  @Test
  public void testSuccess() throws LifecycleException {
    // Shouldn't throw exception
    pmrLifecycleListener.onEnvironmentInit();
  }

  @Test( expected = LifecycleException.class )
  public void testFailure() throws ConfigurationException, LifecycleException {
    ConfigurationException configurationException = mock( ConfigurationException.class );
    when( hadoopConfigurationBootstrap.getProvider() ).thenThrow( configurationException );
    try {
      pmrLifecycleListener.onEnvironmentInit();
    } catch ( LifecycleException e ) {
      assertEquals( configurationException, e.getCause() );
      assertEquals( BaseMessages
          .getString( PMRLifecycleListener.class, PMRLifecycleListener.PMRLIFECYCLE_LISTENER_ERROR_INITIALIZING_SHIM ),
        e.getMessage() );
      assertTrue( e.isSevere() );
      throw e;
    }
  }

  @Test
  public void testShutdownDoesNothing() {
    pmrLifecycleListener.onEnvironmentShutdown();
    verifyNoMoreInteractions( hadoopConfigurationBootstrap );
  }
}
