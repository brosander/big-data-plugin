package org.pentaho.bigdata.api.hbase.table;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by bryan on 1/26/16.
 */
public interface HBaseTableWriteOperationManager extends Closeable {
  boolean isAutoFlush();

  HBasePut createPut( byte[] key );

  HBaseDelete createDelete( byte[] key );

  void flushCommits() throws IOException;
}
