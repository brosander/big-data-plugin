package com.pentaho.big.data.bundles.impl.shim.hbase.table;

import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import org.pentaho.bigdata.api.hbase.table.HBaseDelete;
import org.pentaho.bigdata.api.hbase.table.HBasePut;
import org.pentaho.bigdata.api.hbase.table.HBaseTableWriteOperationManager;

import java.io.IOException;

/**
 * Created by bryan on 1/26/16.
 */
public class HBaseTableWriteOperationManagerImpl implements HBaseTableWriteOperationManager {
  private final HBaseConnectionHandle hBaseConnectionHandle;
  private final boolean autoFlush;

  public HBaseTableWriteOperationManagerImpl( HBaseConnectionHandle hBaseConnectionHandle, boolean autoFlush ) {
    this.hBaseConnectionHandle = hBaseConnectionHandle;
    this.autoFlush = autoFlush;
  }

  @Override public boolean isAutoFlush() {
    return autoFlush;
  }

  @Override public HBasePut createPut( byte[] key ) {
    return new HBasePutImpl( key, hBaseConnectionHandle );
  }

  @Override public HBaseDelete createDelete( byte[] key ) {
    return new HBaseDeleteImpl( hBaseConnectionHandle, key );
  }

  @Override public void flushCommits() throws IOException {
    try {
      hBaseConnectionHandle.getConnection().flushCommitsTargetTable();
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public void close() throws IOException {
    hBaseConnectionHandle.close();
  }
}