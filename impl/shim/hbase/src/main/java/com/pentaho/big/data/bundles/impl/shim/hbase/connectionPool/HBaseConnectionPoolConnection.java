package com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool;

import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;
import org.pentaho.hbase.shim.spi.HBaseConnection;

import java.util.Properties;

/**
 * Created by bryan on 1/25/16.
 */
public class HBaseConnectionPoolConnection extends HBaseConnectionWrapper {
  private HBaseConnectionHandleImpl hBaseConnectionHandle;

  public HBaseConnectionPoolConnection( HBaseConnection delegate ) {
    super( delegate );
  }

  public void init( HBaseConnectionHandleImpl hBaseConnectionHandle ) {
    this.hBaseConnectionHandle = hBaseConnectionHandle;
  }

  @Override public void newSourceTable( String s ) throws Exception {
    super.newSourceTable( s );
    hBaseConnectionHandle.setSourceTable( s );
  }

  @Override public void closeSourceTable() throws Exception {
    super.closeSourceTable();
    hBaseConnectionHandle.setSourceTable( null );
  }

  @Override public void newTargetTable( String s, Properties properties ) throws Exception {
    super.newTargetTable( s, properties );
    hBaseConnectionHandle.setTargetTable( s, properties );
  }

  @Override public void closeTargetTable() throws Exception {
    super.closeTargetTable();
    hBaseConnectionHandle.setTargetTable( null, null );
  }
}
