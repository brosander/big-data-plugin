package com.pentaho.big.data.bundles.impl.shim.hbase.table;

import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import org.pentaho.bigdata.api.hbase.table.HBaseDelete;

import java.io.IOException;

/**
 * Created by bryan on 1/26/16.
 */
public class HBaseDeleteImpl implements HBaseDelete {
  private final HBaseConnectionHandle hBaseConnectionHandle;
  private final byte[] key;

  public HBaseDeleteImpl( HBaseConnectionHandle hBaseConnectionHandle, byte[] key ) {
    this.hBaseConnectionHandle = hBaseConnectionHandle;
    this.key = key;
  }

  @Override public void execute() throws IOException {
    try {
      hBaseConnectionHandle.getConnection().executeTargetTableDelete( key );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }
}
