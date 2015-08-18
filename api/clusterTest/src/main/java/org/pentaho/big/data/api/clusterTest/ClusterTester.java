package org.pentaho.big.data.api.clusterTest;

import org.pentaho.big.data.api.cluster.NamedCluster;

/**
 * Created by bryan on 8/11/15.
 */
public interface ClusterTester {
  void testCluster( NamedCluster namedCluster, ClusterTestProgressCallback clusterTestProgressCallback );
}
