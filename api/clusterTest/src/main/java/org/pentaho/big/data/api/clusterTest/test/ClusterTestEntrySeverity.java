package org.pentaho.big.data.api.clusterTest.test;

import java.util.Collection;

/**
 * Created by bryan on 8/11/15.
 */
public enum ClusterTestEntrySeverity {
  DEBUG, INFO, WARNING, SKIPPED, ERROR, FATAL;

  public static ClusterTestEntrySeverity maxSeverityEntry( Collection<ClusterTestResultEntry> clusterTestResultEntries ) {
    ClusterTestEntrySeverity maxSeverity = null;
    for ( ClusterTestResultEntry clusterTestResultEntry : clusterTestResultEntries ) {
      ClusterTestEntrySeverity severity = clusterTestResultEntry.getSeverity();
      if ( maxSeverity == null || ( severity != null && severity.ordinal() > maxSeverity.ordinal() ) ) {
        maxSeverity = severity;
      }
    }
    return maxSeverity;
  }

  public static ClusterTestEntrySeverity maxSeverityResult( Collection<ClusterTestResult> clusterTestResults ) {
    ClusterTestEntrySeverity maxSeverity = null;
    for ( ClusterTestResult clusterTestResult : clusterTestResults ) {
      ClusterTestEntrySeverity severity = clusterTestResult.getMaxSeverity();
      if ( maxSeverity == null || ( severity != null && severity.ordinal() > maxSeverity.ordinal() ) ) {
        maxSeverity = severity;
      }
    }
    return maxSeverity;
  }
}
