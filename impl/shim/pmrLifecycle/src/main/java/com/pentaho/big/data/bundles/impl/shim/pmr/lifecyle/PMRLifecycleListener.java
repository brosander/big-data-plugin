package com.pentaho.big.data.bundles.impl.shim.pmr.lifecyle;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.di.core.annotations.KettleLifecyclePlugin;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.lifecycle.KettleLifecycleListener;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by bryan on 9/2/15.
 */
@KettleLifecyclePlugin( id = "PMRLifecycleListener", name = "PMR LifecycleListener" )
public class PMRLifecycleListener implements KettleLifecycleListener {
  public static final String PMRLIFECYCLE_LISTENER_ERROR_INITIALIZING_SHIM =
    "PMRLifecycleListener.ErrorInitializingShim";
  private static final Class<?> PKG = PMRLifecycleListener.class;
  private static final Logger logger = LoggerFactory.getLogger( PKG );
  private final HadoopConfigurationBootstrap hadoopConfigurationBootstrap;

  public PMRLifecycleListener() {
    this( HadoopConfigurationBootstrap.getInstance() );
  }

  public PMRLifecycleListener( HadoopConfigurationBootstrap hadoopConfigurationBootstrap ) {
    this.hadoopConfigurationBootstrap = hadoopConfigurationBootstrap;
  }

  @VisibleForTesting HadoopConfigurationBootstrap getHadoopConfigurationBootstrap() {
    return hadoopConfigurationBootstrap;
  }

  @Override public void onEnvironmentInit() throws LifecycleException {
    try {
      logger.info( "About to initialize shim" );
      hadoopConfigurationBootstrap.getProvider();
      logger.info( "Shim initialized" );
      new Exception( "Don't worry about me, I'm harmless" ).printStackTrace();
    } catch ( ConfigurationException e ) {
      throw new LifecycleException( BaseMessages.getString( PKG, PMRLIFECYCLE_LISTENER_ERROR_INITIALIZING_SHIM ), e,
        true );
    }
  }

  @Override public void onEnvironmentShutdown() {

  }
}
