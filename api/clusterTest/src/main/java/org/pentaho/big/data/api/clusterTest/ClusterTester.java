package org.pentaho.big.data.api.clusterTest;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.module.ClusterTestModuleResults;

import java.util.List;
import java.util.Map;

/**
 * Created by bryan on 8/11/15.
 */
public interface ClusterTester {
  void testCluster( NamedCluster namedCluster, ClusterTestProgressCallback clusterTestProgressCallback );
}
