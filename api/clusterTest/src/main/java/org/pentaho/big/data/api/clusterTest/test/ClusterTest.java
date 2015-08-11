package org.pentaho.big.data.api.clusterTest.test;

import org.pentaho.big.data.api.cluster.NamedCluster;

import java.util.Map;
import java.util.Set;

/**
 * Created by bryan on 8/11/15.
 */
public interface ClusterTest {
  String getModule();
  String getId();
  String getName();
  boolean isConfigInitTest();
  Set<String> getDependencies();
  ClusterTestResult runTest( NamedCluster namedCluster );
}
