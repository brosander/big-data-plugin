package org.pentaho.big.data.api.clusterTest.impl;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.ClusterTestProgressCallback;
import org.pentaho.big.data.api.clusterTest.ClusterTester;
import org.pentaho.big.data.api.clusterTest.test.ClusterTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Created by bryan on 8/12/15.
 */
public class ClusterTesterImpl implements ClusterTester {
  private final List<ClusterTest> clusterTests;
  private final ExecutorService executorService;
  private final Map<String, Integer> orderedModules;

  public ClusterTesterImpl( List<ClusterTest> clusterTests, ExecutorService executorService, String orderedModules ) {
    this.clusterTests = clusterTests;
    this.executorService = executorService;
    this.orderedModules = new HashMap<>();
    String[] split = orderedModules.split( "," );
    for ( int module = 0; module < split.length; module++ ) {
      this.orderedModules.put( split[ module ].trim(), module );
    }
  }

  @Override
  public void testCluster( final NamedCluster namedCluster,
                           final ClusterTestProgressCallback clusterTestProgressCallback ) {
    List<ClusterTest> clusterTests = new ArrayList<>( this.clusterTests );
    Collections.sort( clusterTests, new Comparator<ClusterTest>() {

      private Integer nullSafeCompare( Object first, Object second ) {
        if ( first == null ) {
          if ( second == null ) {
            return 0;
          } else {
            return 1;
          }
        }
        if ( second == null ) {
          return -1;
        }
        if ( first.equals( second ) ) {
          return 0;
        }
        return null;
      }

      private int compareModuleNames( String o1Module, String o2Module ) {
        Integer result = nullSafeCompare( o1Module, o2Module );
        if ( result != null ) {
          return result;
        }
        Integer o1OrderNum = orderedModules.get( o1Module );
        Integer o2OrderNum = orderedModules.get( o2Module );
        result = nullSafeCompare( o1OrderNum, o2OrderNum );
        if ( result != null ) {
          return result;
        }
        return o1OrderNum - o2OrderNum;
      }

      @Override public int compare( ClusterTest o1, ClusterTest o2 ) {
        int result = compareModuleNames( o1.getModule(), o2.getModule() );
        if ( result != 0 ) {
          
        }
        String o1Module = o1.getModule();
        String o2Module = o2.getModule();
        if ( o1Module == o2Module ) {

        }
        if ( orderedModules.containsKey( o1Module ) ) {

        }
        return 0;
      }
    } );
    executorService.submit( new Runnable() {
      @Override public void run() {
        new ClusterTestRunner( clusterTests, namedCluster, clusterTestProgressCallback, executorService )
          .runTests();
      }
    } );
  }
}
