package org.pentaho.big.data.api.clusterTest.module;

import org.pentaho.big.data.api.clusterTest.test.ClusterTest;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResult;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;

import java.util.List;
import java.util.Set;

/**
 * Created by bryan on 8/11/15.
 */
public interface ClusterTestModuleResults {
  String getName();

  List<ClusterTestResult> getClusterTestResults();

  Set<ClusterTest> getRunningTests();

  Set<ClusterTest> getOutstandingTests();

  ClusterTestEntrySeverity getMaxSeverity();
}
