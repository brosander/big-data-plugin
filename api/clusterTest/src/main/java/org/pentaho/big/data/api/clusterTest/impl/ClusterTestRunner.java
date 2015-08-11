package org.pentaho.big.data.api.clusterTest.impl;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.ClusterTestProgressCallback;
import org.pentaho.big.data.api.clusterTest.module.ClusterTestModuleResults;
import org.pentaho.big.data.api.clusterTest.module.impl.ClusterTestModuleResultsImpl;
import org.pentaho.big.data.api.clusterTest.test.ClusterTest;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResult;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestDelegateWithMoreDependencies;

import java.util.ArrayList;
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
  private final Map<String, ClusterTestModuleResults> stringClusterTestModuleResultsMap;

  public ClusterTestRunner( Collection<? extends ClusterTest> clusterTests, NamedCluster namedCluster,
                            ClusterTestProgressCallback clusterTestProgressCallback, ExecutorService executorService ) {
    Set<ClusterTest> initTests = new HashSet<>();
    Set<String> initTestIds = new HashSet<>();
    Set<ClusterTest> nonInitTests = new HashSet<>();
    for ( ClusterTest clusterTest : clusterTests ) {
      if ( clusterTest.isConfigInitTest() ) {
        initTests.add( clusterTest );
        initTestIds.add( clusterTest.getId() );
      } else {
        nonInitTests.add( clusterTest );
      }
    }
    this.remainingTests = new HashSet<>( initTests );
    for ( ClusterTest nonInitTest : nonInitTests ) {
      remainingTests.add( new ClusterTestDelegateWithMoreDependencies( nonInitTest, initTestIds ) );
    }
    this.satisfiedDependencies = new HashSet<>();
    this.failedDependencies = new HashSet<>();
    this.namedCluster = namedCluster;
    this.clusterTestProgressCallback = clusterTestProgressCallback;
    this.executorService = executorService;
    Map<String, Set<ClusterTest>> outstandingModuleTests = new HashMap<>();
    Map<String, Set<ClusterTest>> currentlyRunningTests = new HashMap<>();
    Set<String> seenModules = new HashSet<>();
    for ( ClusterTest remainingTest : this.remainingTests ) {
      String module = remainingTest.getModule();
      if ( seenModules.add( module ) ) {
        outstandingModuleTests.put( module, new HashSet<ClusterTest>() );
        currentlyRunningTests.put( module, new HashSet<ClusterTest>() );
      }
      outstandingModuleTests.get( module ).add( remainingTest );
    }
    this.stringClusterTestModuleResultsMap = new HashMap<>();
    for ( ClusterTest remainingTest : this.remainingTests ) {
      String module = remainingTest.getModule();
      ClusterTestModuleResults clusterTestModuleResults = stringClusterTestModuleResultsMap.get( module );
      if ( clusterTestModuleResults == null ) {
        stringClusterTestModuleResultsMap.put( module,
          new ClusterTestModuleResultsImpl( module, new ArrayList<ClusterTestResult>(), new HashSet<ClusterTest>(),
            new HashSet<>( outstandingModuleTests.get( module ) ) ) );
      }
    }
  }

  private void markSkipped( ClusterTest clusterTest ) {
    Set<String> relevantFailed = new HashSet<>( failedDependencies );
    relevantFailed.retainAll( clusterTest.getDependencies() );
    // We had a dependency fail so we need to skip
    failedDependencies.add( clusterTest.getId() );
    String skippingTestModule = clusterTest.getModule();
    ClusterTestModuleResults clusterTestModuleResults = stringClusterTestModuleResultsMap.get( skippingTestModule );
    stringClusterTestModuleResultsMap.put( skippingTestModule,
      ClusterTestModuleResultsImpl.withNewSkippedTest( clusterTestModuleResults, clusterTest, relevantFailed ) );
  }

  private void callbackState() {
    if ( clusterTestProgressCallback != null ) {
      clusterTestProgressCallback
        .onProgress( new ArrayList<>( stringClusterTestModuleResultsMap.values() ) );
    }
  }

  private void runTest( ClusterTest clusterTest, AtomicInteger runningTestCount ) {
    try {
      String clusterTestModule = clusterTest.getModule();
      ClusterTestResult clusterTestResult = clusterTest.runTest( namedCluster );
      ClusterTestEntrySeverity maxSeverity = clusterTestResult.getMaxSeverity();
      String eligibleTestId = clusterTest.getId();
      synchronized ( this ) {
        if ( maxSeverity == ClusterTestEntrySeverity.ERROR || maxSeverity == ClusterTestEntrySeverity.FATAL ) {
          failedDependencies.add( eligibleTestId );
        } else {
          satisfiedDependencies.add( eligibleTestId );
        }

        ClusterTestModuleResults clusterTestModuleResults =
          stringClusterTestModuleResultsMap.get( clusterTestModule );
        stringClusterTestModuleResultsMap.put( clusterTestModule,
          ClusterTestModuleResultsImpl.withCompleteTest( clusterTestModuleResults, clusterTest, clusterTestResult ) );
        callbackState();
        runningTestCount.getAndDecrement();
        notifyAll();
      }
    } catch ( Throwable e ) {
      e.printStackTrace();
    }
  }

  public synchronized List<ClusterTestModuleResults> runTests() {
    callbackState();
    final AtomicInteger runningTestCount = new AtomicInteger();
    while ( remainingTests.size() > 0 || runningTestCount.get() > 0 ) {
      Set<ClusterTest> eligibleTests = new HashSet<>();
      Set<ClusterTest> skippingTests = new HashSet<>();
      Set<String> possibleToSatisfyIds = new HashSet<>( satisfiedDependencies );
      for ( ClusterTest remainingTest : remainingTests ) {
        possibleToSatisfyIds.add( remainingTest.getId() );
      }
      for ( ClusterTestModuleResults clusterTestModuleResults : stringClusterTestModuleResultsMap.values() ) {
        for ( ClusterTest clusterTest : clusterTestModuleResults.getOutstandingTests() ) {
          possibleToSatisfyIds.add( clusterTest.getId() );
        }
        for ( ClusterTest clusterTest : clusterTestModuleResults.getRunningTests() ) {
          possibleToSatisfyIds.add( clusterTest.getId() );
        }
      }
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
        final String eligibleTestModule = eligibleTest.getModule();
        ClusterTestModuleResults clusterTestModuleResults = stringClusterTestModuleResultsMap.get( eligibleTestModule );
        stringClusterTestModuleResultsMap.put( eligibleTestModule,
          ClusterTestModuleResultsImpl.withNewRunningTest( clusterTestModuleResults, eligibleTest ) );

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
    callbackState();
    return new ArrayList<>( stringClusterTestModuleResultsMap.values() );
  }
}
