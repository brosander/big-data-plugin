package org.pentaho.big.data.api.clusterTest.impl;

import org.pentaho.big.data.api.clusterTest.ClusterTestStatus;
import org.pentaho.big.data.api.clusterTest.module.ClusterTestModuleResults;

import java.util.List;

/**
 * Created by bryan on 8/18/15.
 */
public class ClusterTestStatusImpl implements ClusterTestStatus {
  private final List<ClusterTestModuleResults> clusterTestModuleResults;
  private final boolean done;

  public ClusterTestStatusImpl( List<ClusterTestModuleResults> clusterTestModuleResults, boolean done ) {
    this.clusterTestModuleResults = clusterTestModuleResults;
    this.done = done;
  }

  @Override public List<ClusterTestModuleResults> getModuleResults() {
    return clusterTestModuleResults;
  }

  @Override public boolean isDone() {
    return done;
  }

  @Override public String toString() {
    return "ClusterTestStatusImpl{" +
      "clusterTestModuleResults=" + clusterTestModuleResults +
      ", done=" + done +
      '}';
  }
}
