package org.pentaho.big.data.api.clusterTest.impl;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.ClusterTestProgressCallback;
import org.pentaho.big.data.api.clusterTest.module.ClusterTestModuleResults;
import org.pentaho.big.data.api.clusterTest.module.impl.ClusterTestModuleResultsImpl;
import org.pentaho.big.data.api.clusterTest.test.ClusterTest;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResult;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestDelegateWithMoreDependencies;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultEntryImpl;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by bryan on 8/11/15.
 */
public class ClusterTestRunner {
  private final Set<ClusterTest> remainingTests;
  private final NamedCluster namedCluster;
  private final ClusterTestProgressCallback clusterTestProgressCallback;
  private final ExecutorService executorService;
  private final Set<String> satisfiedDependencies;
  private final Set<String> failedDependencies;
  private final List<String> clusterModuleList;
  private final Map<String, List<String>> stringClusterTestModuleToTestIdMap;
  private final Map<String, ClusterTestResult> clusterTestResultMap;
  private final Set<String> outstandingTestIds;
  private final Set<String> runningTestIds;


  public ClusterTestRunner( Collection<? extends ClusterTest> clusterTests, NamedCluster namedCluster,
                            ClusterTestProgressCallback clusterTestProgressCallback, ExecutorService executorService ) {
    clusterModuleList = new ArrayList<>();
    stringClusterTestModuleToTestIdMap = new HashMap<>();
    clusterTestResultMap = new HashMap<>();
    outstandingTestIds = new HashSet<>();
    runningTestIds = new HashSet<>();

    Set<ClusterTest> initTests = new HashSet<>();
    Set<String> initTestIds = new HashSet<>();
    Set<ClusterTest> nonInitTests = new HashSet<>();
    for ( ClusterTest clusterTest : clusterTests ) {
      String clusterTestModule = clusterTest.getModule();
      List<String> clusterIdsForModule = stringClusterTestModuleToTestIdMap.get( clusterTestModule );
      if ( clusterIdsForModule == null ) {
        clusterModuleList.add( clusterTestModule );
        clusterIdsForModule = new ArrayList<>();
        stringClusterTestModuleToTestIdMap.put( clusterTestModule, clusterIdsForModule );
      }
      String clusterTestId = clusterTest.getId();
      clusterIdsForModule.add( clusterTestId );
      if ( clusterTest.isConfigInitTest() ) {
        initTests.add( clusterTest );
        initTestIds.add( clusterTestId );
      } else {
        nonInitTests.add( clusterTest );
      }
    }
    this.remainingTests = new HashSet<>( initTests );
    for ( ClusterTest nonInitTest : nonInitTests ) {
      remainingTests.add( new ClusterTestDelegateWithMoreDependencies( nonInitTest, initTestIds ) );
    }
    for ( ClusterTest remainingTest : remainingTests ) {
      String remainingTestId = remainingTest.getId();
      clusterTestResultMap
        .put( remainingTestId, new ClusterTestResultImpl( remainingTest, new ArrayList<ClusterTestResultEntry>() ) );
      outstandingTestIds.add( remainingTestId );
    }
    this.satisfiedDependencies = new HashSet<>();
    this.failedDependencies = new HashSet<>();
    this.namedCluster = namedCluster;
    this.clusterTestProgressCallback = clusterTestProgressCallback;
    this.executorService = executorService;
  }

  private void markSkipped( ClusterTest clusterTest ) {
    Set<String> relevantFailed = new HashSet<>( failedDependencies );
    relevantFailed.retainAll( clusterTest.getDependencies() );
    // We had a dependency fail so we need to skip
    String clusterTestId = clusterTest.getId();
    failedDependencies.add( clusterTestId );
    outstandingTestIds.remove( clusterTestId );
    runningTestIds.remove( clusterTestId );
    clusterTestResultMap.put( clusterTestId, new ClusterTestResultImpl( clusterTest, Arrays
      .<ClusterTestResultEntry>asList(
        new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.SKIPPED,
          "Skipped test execution " + clusterTest.getName(),
          "The following dependencies either failed or were skipped: " + relevantFailed, null ) ) ) );
  }

  private void callbackState() {
    callbackState( false );
  }

  private void callbackState( boolean done ) {
    if ( clusterTestProgressCallback != null ) {
      List<ClusterTestModuleResults> moduleResults = new ArrayList<>( clusterModuleList.size() );
      for ( String clusterModule : clusterModuleList ) {
        List<ClusterTestResult> clusterTestResults = new ArrayList<>();
        Set<ClusterTest> runningTests = new HashSet<>();
        HashSet<ClusterTest> outstandingTests = new HashSet<>();
        for ( String testId : stringClusterTestModuleToTestIdMap.get( clusterModule ) ) {
          ClusterTestResult clusterTestResult = clusterTestResultMap.get( testId );
          clusterTestResults.add( clusterTestResult );
          if ( runningTestIds.contains( testId ) ) {
            runningTests.add( clusterTestResult.getClusterTest() );
          } else if ( outstandingTestIds.contains( testId ) ) {
            outstandingTests.add( clusterTestResult.getClusterTest() );
          }
        }
        moduleResults
          .add( new ClusterTestModuleResultsImpl( clusterModule, clusterTestResults, runningTests, outstandingTests ) );
      }
      clusterTestProgressCallback
        .onProgress( new ClusterTestStatusImpl( Collections.unmodifiableList( moduleResults ), done ) );
    }
  }

  private void runTest( ClusterTest clusterTest, AtomicInteger runningTestCount ) {
    try {
      ClusterTestResult clusterTestResult = clusterTest.runTest( namedCluster );
      ClusterTestEntrySeverity maxSeverity = clusterTestResult.getMaxSeverity();
      String eligibleTestId = clusterTest.getId();
      synchronized ( this ) {
        if ( maxSeverity == ClusterTestEntrySeverity.ERROR || maxSeverity == ClusterTestEntrySeverity.FATAL ) {
          failedDependencies.add( eligibleTestId );
        } else {
          satisfiedDependencies.add( eligibleTestId );
        }
        if ( clusterTestResult.getClusterTest() == clusterTest ) {
          clusterTestResultMap.put( eligibleTestId, clusterTestResult );
        } else {
          clusterTestResultMap.put( eligibleTestId,
            new ClusterTestResultImpl( clusterTest, clusterTestResult.getClusterTestResultEntries() ) );
        }
        runningTestIds.remove( eligibleTestId );
        callbackState();
        runningTestCount.getAndDecrement();
        notifyAll();
      }
    } catch ( Throwable e ) {
      e.printStackTrace();
    }
  }

  public synchronized void runTests() {
    callbackState();
    final AtomicInteger runningTestCount = new AtomicInteger();
    while ( remainingTests.size() > 0 || runningTestCount.get() > 0 ) {
      Set<ClusterTest> eligibleTests = new HashSet<>();
      Set<ClusterTest> skippingTests = new HashSet<>();
      Set<String> possibleToSatisfyIds = new HashSet<>( satisfiedDependencies );
      for ( ClusterTest remainingTest : remainingTests ) {
        possibleToSatisfyIds.add( remainingTest.getId() );
      }
      possibleToSatisfyIds.addAll( outstandingTestIds );
      possibleToSatisfyIds.addAll( runningTestIds );
      for ( ClusterTest remainingTest : remainingTests ) {
        Set<String> remainingTestDependencies = remainingTest.getDependencies();
        if ( satisfiedDependencies.containsAll( remainingTestDependencies ) ) {
          eligibleTests.add( remainingTest );
        } else if ( !Collections.disjoint( remainingTestDependencies, failedDependencies ) || !possibleToSatisfyIds
          .containsAll( remainingTestDependencies ) ) {
          skippingTests.add( remainingTest );
          markSkipped( remainingTest );
        }
      }
      remainingTests.removeAll( eligibleTests );
      remainingTests.removeAll( skippingTests );
      final int wasRunning = runningTestCount.addAndGet( eligibleTests.size() );
      for ( final ClusterTest eligibleTest : eligibleTests ) {
        String eligibleTestId = eligibleTest.getId();
        outstandingTestIds.remove( eligibleTestId );
        runningTestIds.add( eligibleTestId );

        executorService.submit( new Runnable() {
          @Override public void run() {
            runTest( eligibleTest, runningTestCount );
          }
        } );
      }
      // If we skipped test(s) state has changed and we should rerun immediately, otherwise we can wait until one
      // finishes
      if ( skippingTests.size() == 0 ) {
        if ( wasRunning > 0 ) {
          while ( wasRunning == runningTestCount.get() ) {
            try {
              // Wait until a test finishes
              wait();
            } catch ( InterruptedException e ) {
              // Ignore
            }
          }
        }
      } else {
        callbackState();
      }
    }
    callbackState( true );
  }
}
