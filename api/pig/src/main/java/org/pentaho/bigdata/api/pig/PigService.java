package org.pentaho.bigdata.api.pig;

import java.util.List;

/**
 * Created by bryan on 6/18/15.
 */
public interface PigService {
  boolean isLocalExecutionSupported();

  enum ExecutionMode {
    LOCAL, MAPREDUCE
  }
  int[] executeScript( String scriptPath, ExecutionMode executionMode, List<String> parameters );
}
