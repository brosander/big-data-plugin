package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.pentaho.bigdata.api.hbase.HBaseConnection;
import org.pentaho.bigdata.api.hbase.HBaseService;
import org.pentaho.bigdata.api.hbase.table.HBaseTable;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.hbase.shim.spi.HBaseShim;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Created by bryan on 1/21/16.
 */
public class HBaseConnectionImpl implements HBaseConnection {
  private final HBaseService hBaseService;
  private final HBaseShim hBaseShim;
  private final Properties connectionProps;
  private final LogChannelInterface logChannelInterface;

  public HBaseConnectionImpl( HBaseService hBaseService, HBaseShim hBaseShim, Properties connectionProps,
                              LogChannelInterface logChannelInterface ) {
    this.hBaseService = hBaseService;
    this.hBaseShim = hBaseShim;
    this.connectionProps = connectionProps;
    this.logChannelInterface = logChannelInterface;
  }

  @Override public HBaseService getService() {
    return hBaseService;
  }

  @Override public HBaseTable getTable( String tableName ) throws IOException {
    return null;
  }

  @Override public void checkHBaseAvailable() throws IOException {

  }

  @Override public List<String> listTableNames() throws IOException {
    return null;
  }

  @Override public void close() throws IOException {

  }
}
