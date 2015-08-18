package org.pentaho.big.data.api.clusterTest.module.impl;

import org.pentaho.big.data.api.clusterTest.module.ClusterTestModuleResults;
import org.pentaho.big.data.api.clusterTest.test.ClusterTest;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by bryan on 8/11/15.
 */
public class ClusterTestModuleResultsImpl implements ClusterTestModuleResults {
  private final String name;
  private final List<ClusterTestResult> clusterTestResults;
  private final Set<ClusterTest> runningTests;
  private final Set<ClusterTest> outstandingTests;
  private final ClusterTestEntrySeverity maxSeverity;

  public ClusterTestModuleResultsImpl( String name, List<ClusterTestResult> clusterTestResults,
                                       Set<ClusterTest> runningTests, Set<ClusterTest> outstandingTests ) {
    this.name = name;
    this.runningTests = Collections.unmodifiableSet( new HashSet<>( runningTests ) );
    this.outstandingTests = Collections.unmodifiableSet( new HashSet<>( outstandingTests ) );
    this.clusterTestResults = Collections.unmodifiableList( new ArrayList<>( clusterTestResults ) );
    ClusterTestEntrySeverity maxSeverity = null;
    for ( ClusterTestResult clusterTestResult : clusterTestResults ) {
      ClusterTestEntrySeverity severity = clusterTestResult.getMaxSeverity();
      if ( maxSeverity == null || ( severity != null && severity.ordinal() > maxSeverity.ordinal() ) ) {
        maxSeverity = severity;
      }
    }
    this.maxSeverity = maxSeverity;
  }

  @Override public String name() {
    return name;
  }

  @Override public List<ClusterTestResult> getClusterTestResults() {
    return clusterTestResults;
  }

  @Override public ClusterTestEntrySeverity getMaxSeverity() {
    return maxSeverity;
  }

  @Override public Set<ClusterTest> getRunningTests() {
    return runningTests;
  }

  @Override public Set<ClusterTest> getOutstandingTests() {
    return outstandingTests;
  }
}
