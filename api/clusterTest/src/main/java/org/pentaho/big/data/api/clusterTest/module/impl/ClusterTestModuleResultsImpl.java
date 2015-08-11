package org.pentaho.big.data.api.clusterTest.module.impl;

import org.pentaho.big.data.api.clusterTest.module.ClusterTestModuleResults;
import org.pentaho.big.data.api.clusterTest.test.ClusterTest;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResult;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultEntryImpl;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultImpl;

import java.util.ArrayList;
import java.util.Arrays;
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
    this.runningTests = Collections.unmodifiableSet( new HashSet<ClusterTest>( runningTests ) );
    this.outstandingTests = Collections.unmodifiableSet( new HashSet<ClusterTest>( outstandingTests ) );
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

  public static ClusterTestModuleResults withNewRunningTest( ClusterTestModuleResults clusterTestModuleResults,
                                                             ClusterTest newRunningTest ) {
    Set<ClusterTest> runningTests = new HashSet<>( clusterTestModuleResults.getRunningTests() );
    runningTests.add( newRunningTest );

    Set<ClusterTest> outstandingTests = new HashSet<>( clusterTestModuleResults.getOutstandingTests() );
    outstandingTests.remove( newRunningTest );

    return new ClusterTestModuleResultsImpl( clusterTestModuleResults.name(),
      clusterTestModuleResults.getClusterTestResults(), runningTests, outstandingTests );
  }

  public static ClusterTestModuleResults withCompleteTest( ClusterTestModuleResults clusterTestModuleResults,
                                                           ClusterTest completeTest,
                                                           ClusterTestResult clusterTestResult ) {
    Set<ClusterTest> runningTests = new HashSet<>( clusterTestModuleResults.getRunningTests() );
    runningTests.remove( completeTest );

    List<ClusterTestResult> clusterTestResults = new ArrayList<>( clusterTestModuleResults.getClusterTestResults() );
    clusterTestResults.add( clusterTestResult );

    return new ClusterTestModuleResultsImpl( clusterTestModuleResults.name(),
      clusterTestResults, runningTests, clusterTestModuleResults.getOutstandingTests() );
  }

  public static ClusterTestModuleResults withNewSkippedTest( ClusterTestModuleResults clusterTestModuleResults,
                                                             ClusterTest skippedTest, Set<String> relevantFailed ) {
    Set<ClusterTest> outstandingTests = new HashSet<>( clusterTestModuleResults.getOutstandingTests() );
    outstandingTests.remove( skippedTest );

    List<ClusterTestResult> clusterTestResults = new ArrayList<>( clusterTestModuleResults.getClusterTestResults() );
    clusterTestResults.add( new ClusterTestResultImpl( Arrays.<ClusterTestResultEntry>asList(
      new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.SKIPPED, "Skipped test execution " + skippedTest.getName(),
        "The following dependencies either failed or were skipped: " + relevantFailed, null ) ) ) );

    return new ClusterTestModuleResultsImpl( clusterTestModuleResults.name(),
      clusterTestResults, clusterTestModuleResults.getRunningTests(), outstandingTests );
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
