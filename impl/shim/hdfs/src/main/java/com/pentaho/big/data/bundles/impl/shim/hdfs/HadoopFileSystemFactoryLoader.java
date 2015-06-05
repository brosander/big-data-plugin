package com.pentaho.big.data.bundles.impl.shim.hdfs;

import com.pentaho.big.data.bundles.impl.shim.common.ShimBridgingClassloader;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemFactory;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

/**
 * Created by bryan on 6/4/15.
 */
public class HadoopFileSystemFactoryLoader {
  public static final String HADOOP_FILESYSTEM_FACTORY_IMPL_CANONICAL_NAME =
    HadoopFileSystemFactoryImpl.class.getCanonicalName();
  private static final Logger LOGGER = LoggerFactory.getLogger( HadoopFileSystemFactoryLoader.class );
  private final BundleContext bundleContext;
  private final Map<HadoopConfiguration, ServiceRegistration> hadoopFileSystemFactoryMap =
    new HashMap<>();

  public HadoopFileSystemFactoryLoader( BundleContext bundleContext ) {
    this.bundleContext = bundleContext;
  }

  public void hadoopConfigurationAdded( HadoopConfiguration hadoopConfiguration, Map properties ) {
    if ( hadoopConfiguration == null ) {
      return;
    }
    boolean activeShim = properties != null && Boolean.valueOf( String.valueOf( properties.get( "active" ) ) );
    ShimBridgingClassloader shimBridgingClassloader =
      new ShimBridgingClassloader( hadoopConfiguration.getHadoopShim().getClass().getClassLoader(), bundleContext );
    try {
      HadoopFileSystemFactory hadoopFileSystemFactory = (HadoopFileSystemFactory) Class
        .forName( HADOOP_FILESYSTEM_FACTORY_IMPL_CANONICAL_NAME, true, shimBridgingClassloader )
        .getConstructor( boolean.class, HadoopConfiguration.class ).newInstance( activeShim, hadoopConfiguration );
      ServiceRegistration hadoopFileSystemFactoryServiceRegistration =
        bundleContext.registerService( HadoopFileSystemFactory.class, hadoopFileSystemFactory, new Hashtable<String,
          Object>() );
      hadoopFileSystemFactoryMap.put( hadoopConfiguration, hadoopFileSystemFactoryServiceRegistration );
    } catch ( Exception e ) {
      LOGGER.error( "Unable to register " + hadoopConfiguration.getIdentifier() + " shim", e );
    }
  }

  public void hadoopConfigurationRemoved( HadoopConfiguration hadoopConfiguration, Map properties ) {
    if ( hadoopConfiguration == null ) {
      return;
    }
    ServiceRegistration hadoopFileSystemFactoryServiceRegistration = hadoopFileSystemFactoryMap.remove( hadoopConfiguration );
    if ( hadoopFileSystemFactoryServiceRegistration != null ) {
      hadoopFileSystemFactoryServiceRegistration.unregister();
    }
  }
}
