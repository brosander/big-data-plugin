package org.pentaho.big.data.api.clusterTest.test.impl;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.test.ClusterTest;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResult;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by bryan on 8/17/15.
 */
public class ClusterTestDelegateWithMoreDependencies implements ClusterTest {
  private final ClusterTest delegate;
  private final Set<String> extraDependencies;

  public ClusterTestDelegateWithMoreDependencies( ClusterTest delegate, Set<String> extraDependencies ) {
    this.delegate = delegate;
    this.extraDependencies = new HashSet<>( extraDependencies );
  }

  @Override public String getModule() {
    return delegate.getModule();
  }

  @Override public String getId() {
    return delegate.getId();
  }

  @Override public String getName() {
    return delegate.getName();
  }

  @Override public boolean isConfigInitTest() {
    return delegate.isConfigInitTest();
  }

  @Override public Set<String> getDependencies() {
    HashSet<String> set = new HashSet<String>( extraDependencies );
    set.addAll( delegate.getDependencies() );
    return Collections.unmodifiableSet( set );
  }

  @Override public ClusterTestResult runTest( NamedCluster namedCluster ) {
    return delegate.runTest( namedCluster );
  }
}
