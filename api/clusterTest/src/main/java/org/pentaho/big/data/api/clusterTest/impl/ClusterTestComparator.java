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
        return null;
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
    return o1Module.compareTo( o2Module );
  }

  @Override public int compare( ClusterTest o1, ClusterTest o2 ) {
    Integer result = compareModuleNames( o1.getModule(), o2.getModule() );
    if ( result != 0 ) {
      return result;
    }
    String o1Id = o1.getId();
    String o2Id = o2.getId();
    result = nullSafeCompare( o1Id, o2Id );
    if ( result == null ) {
      result = o1Id.compareTo( o2Id );
    }
    return result;
  }
}
