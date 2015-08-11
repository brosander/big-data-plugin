package org.pentaho.big.data.api.clusterTest.impl;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.ClusterTestProgressCallback;
import org.pentaho.big.data.api.clusterTest.ClusterTester;
import org.pentaho.big.data.api.clusterTest.test.ClusterTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Created by bryan on 8/12/15.
 */
public class ClusterTesterImpl implements ClusterTester {
  private final List<ClusterTest> clusterTests;
  private final ExecutorService executorService;
  private ClusterTestComparator clusterTestComparator;

  public ClusterTesterImpl( List<ClusterTest> clusterTests, ExecutorService executorService,
                            String orderedModulesString ) {
    this.clusterTests = clusterTests;
    this.executorService = executorService;
    HashMap<String, Integer> orderedModules = new HashMap<>();
    String[] split = orderedModulesString.split( "," );
    for ( int module = 0; module < split.length; module++ ) {
      orderedModules.put( split[ module ].trim(), module );
    }
    clusterTestComparator = new ClusterTestComparator( orderedModules );
  }

  @Override
  public void testCluster( final NamedCluster namedCluster,
                           final ClusterTestProgressCallback clusterTestProgressCallback ) {
    final List<ClusterTest> clusterTests = new ArrayList<>( this.clusterTests );
    Collections.sort( clusterTests, clusterTestComparator );
    executorService.submit( new Runnable() {
      @Override public void run() {
        new ClusterTestRunner( clusterTests, namedCluster, clusterTestProgressCallback, executorService )
          .runTests();
      }
    } );
  }
}
