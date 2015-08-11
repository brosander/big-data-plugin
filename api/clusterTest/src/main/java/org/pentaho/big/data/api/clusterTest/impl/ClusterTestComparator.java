package org.pentaho.big.data.api.clusterTest.impl;

import org.pentaho.big.data.api.clusterTest.test.ClusterTest;

import java.util.Comparator;
import java.util.Map;

/**
 * Created by bryan on 8/18/15.
 */
public class ClusterTestComparator implements Comparator<ClusterTest> {
  private final Map<String, Integer> orderedModules;

  public ClusterTestComparator( Map<String, Integer> orderedModules ) {
    this.orderedModules = orderedModules;
  }

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
    Integer result = compareModuleNames( o1.getModule(), o2.getModule() );
    if ( result != 0 ) {
      return result;
    }
    String o1Name = o1.getName();
    String o2Name = o2.getName();
    result = nullSafeCompare( o1Name, o2Name );
    if ( result == null ) {
      result = o1Name.compareTo( o2Name );
    }
    return result;
  }
}
