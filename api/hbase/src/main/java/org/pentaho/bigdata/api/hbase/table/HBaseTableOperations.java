package org.pentaho.bigdata.api.hbase.table;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Created by bryan on 1/26/16.
 */
public interface HBaseTableOperations extends Closeable {
  HBasePut createPut( byte[] key );

  boolean keyExists( byte[] key ) throws IOException;

  HBaseDelete createDelete( byte[] key );

  void flushCommits() throws IOException;
}
