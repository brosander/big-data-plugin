package org.pentaho.big.data.api.clusterTest.test.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.clusterTest.test.ClusterTest;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/20/15.
 */
public class ClusterTestDelegateWithMoreDependenciesTest {
  private ClusterTest delegate;
  private HashSet<String> extraDependencies;
  private ClusterTestDelegateWithMoreDependencies
    clusterTestDelegateWithMoreDependencies;
  private String module;
  private String id;
  private String name;
  private String inheritedDep;
  private String newDep;

  @Before
  public void setup() {
    delegate = mock( ClusterTest.class );
    module = "module";
    id = "id";
    name = "name";
    inheritedDep = "inheritedDep";
    newDep = "newDep";
    when( delegate.getModule() ).thenReturn( module );
    when( delegate.getId() ).thenReturn( id );
    when( delegate.getName() ).thenReturn( name );
    when( delegate.getDependencies() ).thenReturn( new HashSet<>( Arrays.asList( inheritedDep ) ) );
    extraDependencies = new HashSet<>( Arrays.asList( newDep ) );
    clusterTestDelegateWithMoreDependencies =
      new ClusterTestDelegateWithMoreDependencies( delegate, extraDependencies );
  }

  @Test
  public void testGetModule() {
    assertEquals( module, clusterTestDelegateWithMoreDependencies.getModule() );
  }

  @Test
  public void testGetId() {
    assertEquals( id, clusterTestDelegateWithMoreDependencies.getId() );
  }

  @Test
  public void testGetName() {
    assertEquals( name, clusterTestDelegateWithMoreDependencies.getName() );
  }

  @Test
  public void testIsConfigInitTest() {
    when( delegate.isConfigInitTest() ).thenReturn( false ).thenReturn( true );
    assertFalse( clusterTestDelegateWithMoreDependencies.isConfigInitTest() );
    assertTrue( clusterTestDelegateWithMoreDependencies.isConfigInitTest() );
  }

  @Test
  public void testGetDependencies() {
    Set<String> dependencies = clusterTestDelegateWithMoreDependencies.getDependencies();
    assertTrue( dependencies.contains( inheritedDep ) );
    assertTrue( dependencies.contains( newDep ) );
  }

  @Test
  public void testToString() {
    String string = clusterTestDelegateWithMoreDependencies.toString();
    assertTrue( string.contains( delegate.toString() ) );
    assertTrue( string.contains( newDep ) );
  }
}
