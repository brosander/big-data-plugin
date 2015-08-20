package org.pentaho.big.data.api.clusterTest.test.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 8/20/15.
 */
public class BaseClusterTestTest {
  private String module;
  private String id;
  private String name;
  private boolean configInitTest;
  private HashSet<String> dependencies;
  private BaseClusterTest baseClusterTest;

  @Before
  public void setup() {
    module = "module";
    id = "id";
    name = "name";
    configInitTest = true;
    dependencies = new HashSet<>( Arrays.asList( "dependency" ) );
    baseClusterTest = new BaseClusterTest( module, id, name, configInitTest, dependencies ) {
      @Override public List<ClusterTestResultEntry> runTest( NamedCluster namedCluster ) {
        throw new UnsupportedOperationException( "This is a test object, don't run it... ever..." );
      }
    };
  }

  @Test
  public void testGetModule() {
    assertEquals( module, baseClusterTest.getModule() );
  }

  @Test
  public void testGetId() {
    assertEquals( id, baseClusterTest.getId() );
  }

  @Test
  public void testGetName() {
    assertEquals( name, baseClusterTest.getName() );
  }

  @Test
  public void testIsConfigInitTest() {
    assertTrue( baseClusterTest.isConfigInitTest() );
    assertFalse( new BaseClusterTest( module, id, name, dependencies ) {
      @Override public List<ClusterTestResultEntry> runTest( NamedCluster namedCluster ) {
        throw new UnsupportedOperationException( "This is a test object, don't run it... ever..." );
      }
    }.isConfigInitTest() );
  }

  @Test
  public void testToString() {
    String string = baseClusterTest.toString();
    assertTrue( string.contains( module ) );
    assertTrue( string.contains( id ) );
    assertTrue( string.contains( name ) );
    assertTrue( string.contains( "true" ) );
    assertTrue( string.contains( dependencies.toString() ) );
  }
}
