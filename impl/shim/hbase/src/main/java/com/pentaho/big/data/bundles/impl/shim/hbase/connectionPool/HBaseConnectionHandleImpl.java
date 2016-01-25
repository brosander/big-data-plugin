package com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool;

import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by bryan on 1/25/16.
 */
public class HBaseConnectionHandleImpl implements HBaseConnectionHandle {
  private final HBaseConnectionPool hBaseConnectionPool;
  private final HBaseConnectionWrapper hBaseConnection;
  private String sourceTable;
  private String targetTable;
  private Properties targetTableProperties;

  public HBaseConnectionHandleImpl( HBaseConnectionPool hBaseConnectionPool, HBaseConnectionWrapper hBaseConnection ) {
    this.hBaseConnectionPool = hBaseConnectionPool;
    this.hBaseConnection = hBaseConnection;
  }

  @Override public String getSourceTable() {
    return sourceTable;
  }

  public void setSourceTable( String sourceTable ) {
    this.sourceTable = sourceTable;
  }

  @Override public String getTargetTable() {
    return targetTable;
  }

  public void setTargetTable( String targetTable, Properties targetTableProperties ) {
    this.targetTable = targetTable;
    this.targetTableProperties = targetTableProperties;
  }

  @Override public HBaseConnectionWrapper getConnection() {
    return hBaseConnection;
  }

  @Override public void close() throws IOException {
    hBaseConnectionPool.releaseConnection( this );
  }

  @Override public Properties getTargetTableProperties() {
    return targetTableProperties;
  }
}
