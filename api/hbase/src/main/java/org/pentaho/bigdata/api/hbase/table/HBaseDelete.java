package org.pentaho.bigdata.api.hbase.table;

import java.io.IOException;

/**
 * Created by bryan on 1/20/16.
 */
public interface HBaseDelete {
  void execute() throws IOException;
}
