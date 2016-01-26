package com.pentaho.big.data.bundles.impl.shim.hbase.table;

import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import org.pentaho.bigdata.api.hbase.table.HBaseDelete;
import org.pentaho.bigdata.api.hbase.table.HBasePut;
import org.pentaho.bigdata.api.hbase.table.HBaseTableOperations;

import java.io.IOException;
import java.util.List;

/**
 * Created by bryan on 1/26/16.
 */
public class HBaseTableOperationImpl implements HBaseTableOperations {
  private final HBaseConnectionHandle hBaseConnectionHandle;

  public HBaseTableOperationImpl( HBaseConnectionHandle hBaseConnectionHandle ) {
    this.hBaseConnectionHandle = hBaseConnectionHandle;
  }

  @Override public HBasePut createPut( byte[] key ) {
    return new HBasePutImpl( key, hBaseConnectionHandle );
  }

  @Override public boolean keyExists( byte[] key ) throws IOException {
    try {
      return hBaseConnectionHandle.getConnection().sourceTableRowExists( key );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public HBaseDelete createDelete( byte[] key ) {
    return null;
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
