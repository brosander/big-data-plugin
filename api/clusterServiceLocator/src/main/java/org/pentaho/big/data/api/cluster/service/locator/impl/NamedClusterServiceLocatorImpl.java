package org.pentaho.big.data.api.cluster.service.locator.impl;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceFactory;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.api.initializer.ClusterInitializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by bryan on 11/5/15.
 */
public class NamedClusterServiceLocatorImpl implements NamedClusterServiceLocator {
  public static final String SERVICE_RANKING = "service.ranking";
  private final Map<Class<?>, List<ServiceFactoryAndRanking<?>>> serviceFactoryMap;
  private final ReadWriteLock readWriteLock;
  private final ClusterInitializer clusterInitializer;

  public NamedClusterServiceLocatorImpl( ClusterInitializer clusterInitializer ) {
    this( new HashMap<Class<?>, List<ServiceFactoryAndRanking<?>>>(), clusterInitializer );
  }

  @VisibleForTesting
  NamedClusterServiceLocatorImpl( Map<Class<?>, List<ServiceFactoryAndRanking<?>>> serviceFactoryMap,
                                                     ClusterInitializer clusterInitializer ) {
    this.serviceFactoryMap = serviceFactoryMap;
    this.clusterInitializer = clusterInitializer;
    readWriteLock = new ReentrantReadWriteLock();
  }

  public void factoryAdded( NamedClusterServiceFactory<?> namedClusterServiceFactory, Map properties ) {
    if ( namedClusterServiceFactory == null ) {
      return;
    }
    Class<?> serviceClass = namedClusterServiceFactory.getServiceClass();
    Lock writeLock = readWriteLock.writeLock();
    try {
      writeLock.lock();
      List<ServiceFactoryAndRanking<?>> serviceFactories = serviceFactoryMap.get( serviceClass );
      if ( serviceFactories == null ) {
        serviceFactories = new ArrayList<>();
        serviceFactoryMap.put( serviceClass, serviceFactories );
      }
      serviceFactories
        .add( new ServiceFactoryAndRanking( (Integer) properties.get( SERVICE_RANKING ), namedClusterServiceFactory ) );
      Collections.sort( serviceFactories, new Comparator<ServiceFactoryAndRanking<?>>() {
        @Override public int compare( ServiceFactoryAndRanking<?> o1,
                                      ServiceFactoryAndRanking<?> o2 ) {
          if ( o1.ranking == o2.ranking ) {
            return o1.namedClusterServiceFactory.toString().compareTo( o2.namedClusterServiceFactory.toString() );
          }
          return o2.ranking - o1.ranking;
        }
      } );
    } finally {
      writeLock.unlock();
    }
  }

  public void factoryRemoved( NamedClusterServiceFactory<?> namedClusterServiceFactory, Map properties ) {
    if ( namedClusterServiceFactory == null ) {
      return;
    }
    Class<?> serviceClass = namedClusterServiceFactory.getServiceClass();
    Lock writeLock = readWriteLock.writeLock();
    try {
      writeLock.lock();
      List<ServiceFactoryAndRanking<?>> serviceFactories = serviceFactoryMap.remove( serviceClass );
      if ( serviceFactories != null ) {
        List<ServiceFactoryAndRanking<?>> newServiceFactories = new ArrayList<>( serviceFactories.size() );
        for ( ServiceFactoryAndRanking<?> factoryAndRanking : serviceFactories ) {
          if ( !namedClusterServiceFactory.equals( factoryAndRanking.namedClusterServiceFactory ) ) {
            newServiceFactories.add( factoryAndRanking );
          }
        }
        if ( newServiceFactories.size() > 0 ) {
          serviceFactoryMap.put( serviceClass, newServiceFactories );
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override public <T> T getService( NamedCluster namedCluster, Class<T> serviceClass )
    throws ClusterInitializationException {
    clusterInitializer.initialize( namedCluster );
    Lock readLock = readWriteLock.readLock();
    try {
      readLock.lock();
      List<ServiceFactoryAndRanking<?>> serviceFactoryAndRankings = serviceFactoryMap.get( serviceClass );
      if ( serviceFactoryAndRankings != null ) {
        for ( ServiceFactoryAndRanking<?> serviceFactoryAndRanking : serviceFactoryAndRankings ) {
          if ( serviceFactoryAndRanking.namedClusterServiceFactory.canHandle( namedCluster ) ) {
            return serviceClass.cast( serviceFactoryAndRanking.namedClusterServiceFactory.create( namedCluster ) );
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    return null;
  }

  static class ServiceFactoryAndRanking<T> {
    final int ranking;
    final NamedClusterServiceFactory<T> namedClusterServiceFactory;

    ServiceFactoryAndRanking( Integer ranking, NamedClusterServiceFactory<T> namedClusterServiceFactory ) {
      if ( ranking == null ) {
        this.ranking = 0;
      } else {
        this.ranking = ranking;
      }
      this.namedClusterServiceFactory = namedClusterServiceFactory;
    }
  }
}
