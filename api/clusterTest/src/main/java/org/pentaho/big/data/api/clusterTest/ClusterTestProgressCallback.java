package org.pentaho.big.data.api.clusterTest;

import org.pentaho.big.data.api.clusterTest.module.ClusterTestModuleResults;

import java.util.List;

/**
 * Created by bryan on 8/11/15.
 */
public interface ClusterTestProgressCallback {
  void onProgress( List<ClusterTestModuleResults> moduleResults );
}
