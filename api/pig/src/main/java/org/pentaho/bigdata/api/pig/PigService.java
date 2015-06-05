package org.pentaho.bigdata.api.pig;

import java.util.List;

/**
 * Created by bryan on 6/18/15.
 */
public interface PigService {
  enum ExecutionMode {
    LOCAL, MAP_REDUCE
  }
  void runScript( String scriptPath, ExecutionMode executionMode, List<String> parameters );
}
