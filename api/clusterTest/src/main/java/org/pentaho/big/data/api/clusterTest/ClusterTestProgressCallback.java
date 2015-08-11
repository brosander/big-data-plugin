package org.pentaho.big.data.api.clusterTest;

/**
 * Created by bryan on 8/11/15.
 */
public interface ClusterTestProgressCallback {
  void onProgress( ClusterTestStatus clusterTestStatus );
}
