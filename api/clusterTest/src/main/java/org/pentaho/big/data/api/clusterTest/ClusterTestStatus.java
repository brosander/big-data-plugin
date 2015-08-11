package org.pentaho.big.data.api.clusterTest;

import org.pentaho.big.data.api.clusterTest.module.ClusterTestModuleResults;

import java.util.List;

/**
 * Created by bryan on 8/18/15.
 */
public interface ClusterTestStatus {
  List<ClusterTestModuleResults> getModuleResults();

  boolean isDone();
}
