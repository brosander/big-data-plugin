package org.pentaho.big.data.api.clusterTest.test;

import java.util.List;

/**
 * Created by bryan on 8/11/15.
 */
public interface ClusterTestResult {
  ClusterTest getClusterTest();

  List<ClusterTestResultEntry> getClusterTestResultEntries();

  ClusterTestEntrySeverity getMaxSeverity();
}
