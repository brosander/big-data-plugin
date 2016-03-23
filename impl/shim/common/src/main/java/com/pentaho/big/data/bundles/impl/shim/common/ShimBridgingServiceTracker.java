/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package com.pentaho.big.data.bundles.impl.shim.common;

import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

/**
 * Created by bryan on 7/7/15.
 */
public class ShimBridgingServiceTracker {
  private static final Logger LOGGER = LoggerFactory.getLogger( ShimBridgingServiceTracker.class );
  private final Map<Object, ShimRef> serviceRegistrationMap = new HashMap<>();
  private final boolean callListeners;

  public ShimBridgingServiceTracker() {
    this( true );
  }

  public ShimBridgingServiceTracker( boolean callListeners ) {
    this.callListeners = callListeners;
  }

  public boolean registerWithClassloader( Object serviceKey, Class iface, String className, BundleContext bundleContext,
                                          ClassLoader parentClassloader, Class<?>[] argTypes,
                                          Object[] args )
    throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
    InstantiationException {
    return registerWithClassloader( serviceKey, iface, className, bundleContext, parentClassloader, argTypes, args,
      new Hashtable<String, Object>() );
  }

  public boolean registerWithClassloader( Object serviceKey, Class iface, String className, BundleContext bundleContext,
                                          ClassLoader parentClassloader, Class<?>[] argTypes,
                                          Object[] args, Dictionary<String, ?> properties )
    throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
    InstantiationException {
    if ( serviceKey == null ) {
      LOGGER.warn( "Skipped registering " + serviceKey + " as " + iface.getCanonicalName()
        + " because it was null." );
      return false;
    }
    if ( serviceRegistrationMap.containsKey( serviceKey ) ) {
      LOGGER.warn( "Skipped registering " + serviceKey + " as " + iface.getCanonicalName()
        + " because it was already registered." );
      return false;
    }
    Object service = Class.forName( className, true, new ShimBridgingClassloader( parentClassloader, bundleContext ) )
      .getConstructor( argTypes ).newInstance( args );
    serviceRegistrationMap.put( serviceKey, new ShimRef( bundleContext, bundleContext.registerService( iface,
      service, properties ), iface, service ) );
    if ( callListeners ) {
      try {
        ServiceReference<?>[] serviceReferences = bundleContext.getAllServiceReferences( ShimBridgingServiceTrackerListener.class.getCanonicalName(), null );
        if ( serviceReferences != null ) {
          for ( ServiceReference<?> serviceReference : serviceReferences ) {
            try {
              ( (ShimBridgingServiceTrackerListener) bundleContext.getService( serviceReference ) )
                .onRegister( iface, service );
            } catch ( Exception e ) {
              LOGGER.error( "Error notifying " + serviceReference + " of new service " + service, e );
            }
          }
        }
      } catch ( InvalidSyntaxException e ) {
        LOGGER.error( "Error notifying listeners of new " + iface, e );
      }
    }
    LOGGER.debug( "Registered " + serviceKey + " as " + iface.getCanonicalName() + " successfully!!" );
    return true;
  }

  public boolean unregister( Object serviceKey ) {
    if ( serviceKey == null ) {
      LOGGER.warn( "Skipped unregistering " + serviceKey + " because it was null." );
      return false;
    }
    ShimRef shimRef = serviceRegistrationMap.remove( serviceKey );
    if ( shimRef != null ) {
      if ( callListeners ) {
        try {
          for ( ServiceReference<?> serviceReference : shimRef.bundleContext
            .getAllServiceReferences( ShimBridgingServiceTrackerListener.class.getCanonicalName(), null ) ) {
            try {
              ( (ShimBridgingServiceTrackerListener) shimRef.bundleContext.getService( serviceReference ) )
                .onUnregister( shimRef.iface, shimRef.service );
            } catch ( Exception e ) {
              LOGGER.error( "Error notifying " + serviceReference + " of new service " + shimRef.service, e );
            }
          }
        } catch ( InvalidSyntaxException e ) {
          LOGGER.error( "Error notifying listeners of new " + shimRef.iface, e );
        }
      }
      shimRef.serviceRegistration.unregister();
      LOGGER.debug( "Unregistered " + serviceKey + " as " + shimRef.iface + " successfully!!" );
      return true;
    } else {
      LOGGER.warn( "Skipped unregistering " + serviceKey + " because it was already registered." );
      return false;
    }
  }

  private static final class ShimRef {
    private final BundleContext bundleContext;
    private final ServiceRegistration serviceRegistration;
    private final Class<?> iface;
    private final Object service;

    private ShimRef( BundleContext bundleContext, ServiceRegistration reference, Class<?> iface, Object service ) {
      this.bundleContext = bundleContext;
      this.serviceRegistration = reference;
      this.iface = iface;
      this.service = service;
    }
  }
}
