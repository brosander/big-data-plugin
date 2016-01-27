package com.pentaho.big.data.bundles.impl.shim.hbase;

import com.pentaho.big.data.bundles.impl.shim.common.ShimBridgingServiceTracker;
import org.osgi.framework.BundleContext;
import org.pentaho.bigdata.api.hbase.HBaseService;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.hadoop.HadoopConfigurationListener;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by bryan on 1/27/16.
 */
public class HBaseServiceLoader implements HadoopConfigurationListener {
  private static final Logger LOGGER = LoggerFactory.getLogger( HBaseServiceLoader.class );
  public static final String HBASE_SERVICE_FACTORY_CANONICAL_NAME = HBaseServiceFactory.class.getCanonicalName();
  private final BundleContext bundleContext;
  private final ShimBridgingServiceTracker shimBridgingServiceTracker;

  public HBaseServiceLoader( BundleContext bundleContext,
                             ShimBridgingServiceTracker shimBridgingServiceTracker )
    throws ConfigurationException {
    this( bundleContext, shimBridgingServiceTracker, HadoopConfigurationBootstrap.getInstance() );
  }

  public HBaseServiceLoader( BundleContext bundleContext,
                             ShimBridgingServiceTracker shimBridgingServiceTracker,
                             HadoopConfigurationBootstrap hadoopConfigurationBootstrap )
    throws ConfigurationException {
    this.bundleContext = bundleContext;
    this.shimBridgingServiceTracker = shimBridgingServiceTracker;
    hadoopConfigurationBootstrap.registerHadoopConfigurationListener( this );
  }

  @Override public void onConfigurationOpen( HadoopConfiguration hadoopConfiguration, boolean defaultConfiguration ) {
    try {
      shimBridgingServiceTracker.registerWithClassloader( hadoopConfiguration, HBaseService.class,
        HBASE_SERVICE_FACTORY_CANONICAL_NAME,
        bundleContext, hadoopConfiguration.getHadoopShim().getClass().getClassLoader(),
        new Class<?>[] { boolean.class, HadoopConfiguration.class },
        new Object[] { defaultConfiguration, hadoopConfiguration } );
    } catch ( Exception e ) {
      LOGGER.error( "Unable to register " + hadoopConfiguration.getIdentifier() + " shim", e );
    }
  }

  @Override public void onConfigurationClose( HadoopConfiguration hadoopConfiguration ) {
    shimBridgingServiceTracker.unregister( hadoopConfiguration );
  }

  @Override public void onClassLoaderAvailable( ClassLoader classLoader ) {
    // Noop
  }
}
