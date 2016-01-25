package com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool;

import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;

import java.io.Closeable;
import java.util.Properties;

/**
 * Created by bryan on 1/25/16.
 */
public interface HBaseConnectionHandle extends Closeable {
  HBaseConnectionWrapper getConnection();

  String getSourceTable();

  String getTargetTable();

  Properties getTargetTableProperties();
}
