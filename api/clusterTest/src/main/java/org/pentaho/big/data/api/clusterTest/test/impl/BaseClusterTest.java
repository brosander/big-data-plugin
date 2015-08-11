package org.pentaho.big.data.api.clusterTest.test.impl;

import org.pentaho.big.data.api.clusterTest.test.ClusterTest;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by bryan on 8/11/15.
 */
public abstract class BaseClusterTest implements ClusterTest {
  private final String module;
  private final String id;
  private final String name;
  private final boolean configInitTest;
  private final Set<String> dependencies;

  public BaseClusterTest( String module, String id, String name, Set<String> dependencies ) {
    this( module, id, name, false, dependencies );
  }

  public BaseClusterTest( String module, String id, String name, boolean configInitTest, Set<String> dependencies ) {
    this.module = module;
    this.id = id;
    this.name = name;
    this.configInitTest = configInitTest;
    this.dependencies = Collections.unmodifiableSet( new HashSet<>( dependencies ) );
  }

  @Override public String getModule() {
    return module;
  }

  @Override public String getId() {
    return id;
  }

  @Override public String getName() {
    return name;
  }

  @Override public Set<String> getDependencies() {
    return dependencies;
  }

  @Override public boolean isConfigInitTest() {
    return configInitTest;
  }
}
