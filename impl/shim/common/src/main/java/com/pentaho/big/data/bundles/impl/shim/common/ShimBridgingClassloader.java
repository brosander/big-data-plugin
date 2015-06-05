package com.pentaho.big.data.bundles.impl.shim.common;

import org.apache.commons.io.IOUtils;
import org.osgi.framework.BundleContext;
import org.osgi.framework.wiring.BundleWiring;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 * Created by bryan on 6/4/15.
 */
public class ShimBridgingClassloader extends ClassLoader {
  private final BundleWiring bundleWiring;
  private final PublicLoadResolveClassLoader bundleWiringClassloader;

  public ShimBridgingClassloader( ClassLoader parentClassLoader, BundleContext bundleContext ) {
    super( parentClassLoader );
    this.bundleWiring = (BundleWiring) bundleContext.getBundle().adapt( BundleWiring.class );
    this.bundleWiringClassloader = new PublicLoadResolveClassLoader( bundleWiring.getClassLoader() );
  }

  @Override
  protected Class<?> findClass( String name ) throws ClassNotFoundException {
    int lastIndexOfDot = name.lastIndexOf( '.' );
    String translatedPath = "/" + name.substring( 0, lastIndexOfDot ).replace( '.', '/' );
    String translatedName = name.substring( lastIndexOfDot + 1 ) + ".class";
    List<URL> entries = bundleWiring.findEntries( translatedPath, translatedName, 0 );
    if ( entries.size() == 1 ) {
      byte[] bytes;
      try ( ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream() ) {
        IOUtils.copy( entries.get( 0 ).openStream(), byteArrayOutputStream );
        bytes = byteArrayOutputStream.toByteArray();
      } catch ( IOException e ) {
        throw new ClassNotFoundException( "Unable to define class", e );
      }
      return defineClass( name, bytes, 0, bytes.length );
    }
    throw new ClassNotFoundException();
  }

  @Override
  public Class<?> loadClass( String name, boolean resolve ) throws ClassNotFoundException {
    Class<?> result = null;
    synchronized ( this ) {
      result = findLoadedClass( name );
    }
    if ( result == null ) {
      try {
        result = findClass( name );
      } catch ( Exception e ) {

      }
    }
    if ( result == null ) {
      try {
        return bundleWiringClassloader.loadClass( name, resolve );
      } catch ( Exception e ) {

      }
    }
    if ( result == null ) {
      return super.loadClass( name, resolve );
    }
    if ( resolve ) {
      resolveClass( result );
    }
    return result;
  }

  /**
   * Trivial classloader subclass that lets us call loadClass with a resolve parameter
   */
  private static class PublicLoadResolveClassLoader extends ClassLoader {
    public PublicLoadResolveClassLoader( ClassLoader parent ) {
      super( parent );
    }

    @Override
    public Class<?> loadClass( String name, boolean resolve ) throws ClassNotFoundException {
      return super.loadClass( name, resolve );
    }
  }
}

