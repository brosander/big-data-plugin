package com.pentaho.big.data.bundles.impl.shim.hbase;

import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionPool;
import com.pentaho.big.data.bundles.impl.shim.hbase.table.HBaseTableImpl;
import org.pentaho.bigdata.api.hbase.HBaseConnection;
import org.pentaho.bigdata.api.hbase.HBaseService;
import org.pentaho.bigdata.api.hbase.table.HBaseTable;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;
import org.pentaho.hbase.shim.spi.HBaseShim;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Created by bryan on 1/21/16.
 */
public class HBaseConnectionImpl implements HBaseConnection {
  private final HBaseServiceImpl hBaseService;
  private final HBaseConnectionPool hBaseConnectionPool;
  private final HBaseBytesUtilShim hBaseBytesUtilShim;

  public HBaseConnectionImpl( HBaseServiceImpl hBaseService, HBaseShim hBaseShim, HBaseBytesUtilShim hBaseBytesUtilShim,
                              Properties connectionProps, LogChannelInterface logChannelInterface ) throws IOException {
    this.hBaseService = hBaseService;
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
    this.hBaseConnectionPool = new HBaseConnectionPool( hBaseShim, connectionProps, logChannelInterface );
  }

  @Override public HBaseService getService() {
    return hBaseService;
  }

  @Override public HBaseTable getTable( String tableName ) throws IOException {
    return new HBaseTableImpl( hBaseConnectionPool, hBaseService.getHBaseValueMetaInterfaceFactory(),
      hBaseBytesUtilShim, tableName );
  }

  @Override public void checkHBaseAvailable() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      hBaseConnectionHandle.getConnection().checkHBaseAvailable();
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public List<String> listTableNames() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      return hBaseConnectionHandle.getConnection().listTableNames();
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public void close() throws IOException {
    hBaseConnectionPool.close();
  }
}
