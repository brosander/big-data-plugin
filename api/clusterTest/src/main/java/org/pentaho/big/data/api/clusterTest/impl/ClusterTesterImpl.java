package org.pentaho.big.data.api.clusterTest.impl;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.ClusterTestProgressCallback;
import org.pentaho.big.data.api.clusterTest.ClusterTester;
import org.pentaho.big.data.api.clusterTest.module.ClusterTestModuleResults;
import org.pentaho.big.data.api.clusterTest.test.ClusterTest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Created by bryan on 8/12/15.
 */
public class ClusterTesterImpl implements ClusterTester {
  private final List<ClusterTest> clusterTests;
  private final ExecutorService executorService;

  public ClusterTesterImpl( List<ClusterTest> clusterTests, ExecutorService executorService ) {
    this.clusterTests = clusterTests;
    this.executorService = executorService;
  }

  @Override
  public void testCluster( final NamedCluster namedCluster, final ClusterTestProgressCallback clusterTestProgressCallback ) {
    executorService.submit( new Runnable() {
      @Override public void run() {
        new ClusterTestRunner( clusterTests, namedCluster, clusterTestProgressCallback, executorService )
          .runTests();
      }
    } );
  }
}
