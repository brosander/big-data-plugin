package com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool;

import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;

import java.io.IOException;

/**
 * Created by bryan on 1/25/16.
 */
public class HBaseConnectionHandleImpl implements HBaseConnectionHandle {
  private final HBaseConnectionPool hBaseConnectionPool;
  private HBaseConnectionWrapper hBaseConnection;

  public HBaseConnectionHandleImpl( HBaseConnectionPool hBaseConnectionPool, HBaseConnectionWrapper hBaseConnection ) {
    this.hBaseConnectionPool = hBaseConnectionPool;
    this.hBaseConnection = hBaseConnection;
  }

  @Override public HBaseConnectionWrapper getConnection() {
    return hBaseConnection;
  }

  @Override public void close() throws IOException {
    HBaseConnectionWrapper hBaseConnection = this.hBaseConnection;
    this.hBaseConnection = null;
    hBaseConnectionPool.releaseConnection( hBaseConnection );
  }
}
