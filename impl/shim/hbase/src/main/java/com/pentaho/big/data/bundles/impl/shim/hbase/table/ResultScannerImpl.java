package com.pentaho.big.data.bundles.impl.shim.hbase.table;

import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;
import com.pentaho.big.data.bundles.impl.shim.hbase.ResultImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import org.pentaho.bigdata.api.hbase.Result;
import org.pentaho.bigdata.api.hbase.table.ResultScanner;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import java.io.IOException;

/**
 * Created by bryan on 1/25/16.
 */
public class ResultScannerImpl implements ResultScanner {
  private final HBaseConnectionHandle hBaseConnectionHandle;
  private final HBaseConnectionWrapper hBaseConnectionWrapper;
  private final HBaseBytesUtilShim hBaseBytesUtilShim;

  public ResultScannerImpl( HBaseConnectionHandle hBaseConnectionHandle, HBaseBytesUtilShim hBaseBytesUtilShim ) {
    this.hBaseConnectionHandle = hBaseConnectionHandle;
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
    hBaseConnectionWrapper = hBaseConnectionHandle.getConnection();
  }

  @Override public Result next() throws IOException {
    try {
      if ( !hBaseConnectionWrapper.resultSetNextRow() ) {
        return null;
      }
      return new ResultImpl( hBaseConnectionWrapper.getCurrentResult(), hBaseBytesUtilShim );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public void close() throws IOException {
    hBaseConnectionHandle.close();
  }
}
