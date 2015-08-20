package org.pentaho.big.data.api.clusterTest.test.impl;

import org.pentaho.big.data.api.clusterTest.test.ClusterTest;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResult;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by bryan on 8/12/15.
 */
public class ClusterTestResultImpl implements ClusterTestResult {
  private final ClusterTest clusterTest;
  private final List<ClusterTestResultEntry> clusterTestResultEntries;
  private final ClusterTestEntrySeverity maxSeverity;
  private final long timeTaken;

  public ClusterTestResultImpl( ClusterTest clusterTest, List<ClusterTestResultEntry> clusterTestResultEntries,
                                long timeTaken ) {
    this.clusterTest = clusterTest;
    this.clusterTestResultEntries = Collections.unmodifiableList( new ArrayList<>( clusterTestResultEntries ) );
    this.timeTaken = timeTaken;
    this.maxSeverity = ClusterTestEntrySeverity.maxSeverityEntry( clusterTestResultEntries );
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

  @Override public long getTimeTaken() {
    return timeTaken;
  }

  @Override public String toString() {
    return "ClusterTestResultImpl{" +
      "clusterTest=" + clusterTest +
      ", clusterTestResultEntries=" + clusterTestResultEntries +
      ", maxSeverity=" + maxSeverity +
      ", timeTaken=" + timeTaken +
      '}';
  }
}
