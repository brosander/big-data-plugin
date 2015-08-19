package org.pentaho.big.data.api.clusterTest.test.impl;

import org.pentaho.big.data.api.clusterTest.test.ClusterTest;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResult;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;

import java.util.List;

/**
 * Created by bryan on 8/12/15.
 */
public class ClusterTestResultImpl implements ClusterTestResult {
  private final ClusterTest clusterTest;
  private final List<ClusterTestResultEntry> clusterTestResultEntries;
  private final ClusterTestEntrySeverity maxSeverity;

  public ClusterTestResultImpl( ClusterTest clusterTest, List<ClusterTestResultEntry> clusterTestResultEntries ) {
    this.clusterTest = clusterTest;
    this.clusterTestResultEntries = clusterTestResultEntries;
    ClusterTestEntrySeverity maxSeverity = null;
    for ( ClusterTestResultEntry clusterTestResultEntry : clusterTestResultEntries ) {
      ClusterTestEntrySeverity severity = clusterTestResultEntry.getSeverity();
      if ( maxSeverity == null || ( severity != null && severity.ordinal() > maxSeverity.ordinal() ) ) {
        maxSeverity = severity;
      }
    }
    this.maxSeverity = maxSeverity;
  }

  @Override public ClusterTestEntrySeverity getMaxSeverity() {
    return maxSeverity;
  }

  @Override public List<ClusterTestResultEntry> getClusterTestResultEntries() {
    return clusterTestResultEntries;
  }

  @Override public ClusterTest getClusterTest() {
    return clusterTest;
  }

  @Override public String toString() {
    return "ClusterTestResultImpl{" +
      "clusterTest=" + clusterTest +
      ", clusterTestResultEntries=" + clusterTestResultEntries +
      ", maxSeverity=" + maxSeverity +
      '}';
  }
}
