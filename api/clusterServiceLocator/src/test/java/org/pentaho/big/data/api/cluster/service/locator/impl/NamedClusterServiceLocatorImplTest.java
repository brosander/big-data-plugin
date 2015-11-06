package org.pentaho.big.data.api.cluster.service.locator.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceFactory;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.api.initializer.ClusterInitializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.big.data.api.cluster.service.locator.impl.NamedClusterServiceLocatorImpl.SERVICE_RANKING;

/**
 * Created by bryan on 11/6/15.
 */
public class NamedClusterServiceLocatorImplTest {
  private HashMap<Class<?>, List<NamedClusterServiceLocatorImpl.ServiceFactoryAndRanking<?>>> serviceFactoryMap;
  private NamedClusterServiceLocatorImpl serviceLocator;
  private NamedCluster namedCluster;
  private NamedClusterServiceFactory namedClusterServiceFactory;
  private NamedClusterServiceFactory namedClusterServiceFactory2;
  private NamedClusterServiceFactory namedClusterServiceFactory3;
  private NamedClusterServiceFactory namedClusterServiceFactory4;
  private Object value;
  private ClusterInitializer clusterInitializer;

  @Before
  public void setup() {
    serviceFactoryMap = new HashMap<>();
    clusterInitializer = mock( ClusterInitializer.class );
    serviceLocator = new NamedClusterServiceLocatorImpl( serviceFactoryMap, clusterInitializer );
    namedCluster = mock( NamedCluster.class );
    namedClusterServiceFactory = mock( NamedClusterServiceFactory.class );
    namedClusterServiceFactory2 = mock( NamedClusterServiceFactory.class );
    namedClusterServiceFactory3 = mock( NamedClusterServiceFactory.class );
    namedClusterServiceFactory4 = mock( NamedClusterServiceFactory.class );
    when( namedClusterServiceFactory.getServiceClass() ).thenReturn( Object.class );
    when( namedClusterServiceFactory2.getServiceClass() ).thenReturn( Object.class );
    when( namedClusterServiceFactory3.getServiceClass() ).thenReturn( Object.class );
    when( namedClusterServiceFactory4.getServiceClass() ).thenReturn( Object.class );
    when( namedClusterServiceFactory.toString() ).thenReturn( "d" );
    when( namedClusterServiceFactory2.toString() ).thenReturn( "b" );
    when( namedClusterServiceFactory3.toString() ).thenReturn( "a" );
    when( namedClusterServiceFactory4.toString() ).thenReturn( "c" );
    serviceLocator.factoryAdded( namedClusterServiceFactory, Collections.singletonMap( SERVICE_RANKING, 2 ) );
    serviceLocator.factoryAdded( namedClusterServiceFactory2, Collections.singletonMap( SERVICE_RANKING, 1 ) );
    serviceLocator.factoryAdded( namedClusterServiceFactory3, Collections.emptyMap() );
    serviceLocator.factoryAdded( namedClusterServiceFactory4, Collections.singletonMap( SERVICE_RANKING, 1 ) );
    value = new Object();
  }

  @Test
  public void testNoArgConstructor() throws ClusterInitializationException {
    assertNull( new NamedClusterServiceLocatorImpl( clusterInitializer ).getService( namedCluster, Object.class ) );
  }

  @Test
  public void testFactoryAddedRemoved() {
    List<NamedClusterServiceLocatorImpl.ServiceFactoryAndRanking<?>> serviceFactoryAndRankings =
      serviceFactoryMap.get( Object.class );
    assertEquals( 4, serviceFactoryAndRankings.size() );
    assertEquals( namedClusterServiceFactory, serviceFactoryAndRankings.get( 0 ).namedClusterServiceFactory );
    assertEquals( namedClusterServiceFactory2, serviceFactoryAndRankings.get( 1 ).namedClusterServiceFactory );
    assertEquals( namedClusterServiceFactory4, serviceFactoryAndRankings.get( 2 ).namedClusterServiceFactory );
    assertEquals( namedClusterServiceFactory3, serviceFactoryAndRankings.get( 3 ).namedClusterServiceFactory );

    serviceLocator.factoryRemoved( namedClusterServiceFactory, Collections.emptyMap() );
    serviceFactoryAndRankings = serviceFactoryMap.get( Object.class );
    assertEquals( 3, serviceFactoryAndRankings.size() );
    assertEquals( namedClusterServiceFactory2, serviceFactoryAndRankings.get( 0 ).namedClusterServiceFactory );
    assertEquals( namedClusterServiceFactory4, serviceFactoryAndRankings.get( 1 ).namedClusterServiceFactory );
    assertEquals( namedClusterServiceFactory3, serviceFactoryAndRankings.get( 2 ).namedClusterServiceFactory );

    serviceLocator.factoryRemoved( namedClusterServiceFactory, Collections.emptyMap() );
    serviceFactoryAndRankings = serviceFactoryMap.get( Object.class );
    assertEquals( 3, serviceFactoryAndRankings.size() );
    assertEquals( namedClusterServiceFactory2, serviceFactoryAndRankings.get( 0 ).namedClusterServiceFactory );
    assertEquals( namedClusterServiceFactory4, serviceFactoryAndRankings.get( 1 ).namedClusterServiceFactory );
    assertEquals( namedClusterServiceFactory3, serviceFactoryAndRankings.get( 2 ).namedClusterServiceFactory );

    serviceLocator.factoryRemoved( namedClusterServiceFactory2, Collections.emptyMap() );
    serviceFactoryAndRankings = serviceFactoryMap.get( Object.class );
    assertEquals( 2, serviceFactoryAndRankings.size() );
    assertEquals( namedClusterServiceFactory4, serviceFactoryAndRankings.get( 0 ).namedClusterServiceFactory );
    assertEquals( namedClusterServiceFactory3, serviceFactoryAndRankings.get( 1 ).namedClusterServiceFactory );

    serviceLocator.factoryRemoved( namedClusterServiceFactory4, Collections.emptyMap() );
    serviceFactoryAndRankings = serviceFactoryMap.get( Object.class );
    assertEquals( 1, serviceFactoryAndRankings.size() );
    assertEquals( namedClusterServiceFactory3, serviceFactoryAndRankings.get( 0 ).namedClusterServiceFactory );

    serviceLocator.factoryRemoved( namedClusterServiceFactory3, Collections.emptyMap() );
    serviceFactoryAndRankings = serviceFactoryMap.get( Object.class );
    assertNull( serviceFactoryAndRankings );
  }

  @Test
  public void testGetServiceFirst() throws ClusterInitializationException {
    when( namedClusterServiceFactory.canHandle( namedCluster ) ).thenReturn( true );
    when( namedClusterServiceFactory.create( namedCluster ) ).thenReturn( value );
    assertEquals( value, serviceLocator.getService( namedCluster, Object.class ) );
    verify( namedClusterServiceFactory2, never() ).create( namedCluster );
    verify( namedClusterServiceFactory3, never() ).create( namedCluster );
    verify( namedClusterServiceFactory4, never() ).create( namedCluster );
    verify( clusterInitializer ).initialize( namedCluster );
  }

  @Test
  public void testGetServiceLast() throws ClusterInitializationException {
    when( namedClusterServiceFactory3.canHandle( namedCluster ) ).thenReturn( true );
    when( namedClusterServiceFactory3.create( namedCluster ) ).thenReturn( value );
    assertEquals( value, serviceLocator.getService( namedCluster, Object.class ) );
    verify( namedClusterServiceFactory, never() ).create( namedCluster );
    verify( namedClusterServiceFactory2, never() ).create( namedCluster );
    verify( namedClusterServiceFactory4, never() ).create( namedCluster );
    verify( clusterInitializer ).initialize( namedCluster );
  }

  @Test
  public void testNullAdded() {
    serviceFactoryMap.clear();
    serviceLocator.factoryAdded( null, Collections.emptyMap() );
    assertEquals( 0, serviceFactoryMap.size() );
  }

  @Test
  public void testNullRemoved() {
    serviceFactoryMap.clear();
    serviceLocator.factoryRemoved( null, Collections.emptyMap() );
    assertEquals( 0, serviceFactoryMap.size() );
  }
}
