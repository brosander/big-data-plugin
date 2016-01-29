package org.pentaho.bigdata.api.hbase.table;

import org.pentaho.bigdata.api.hbase.Result;

import java.io.IOException;

/**
 * Created by bryan on 1/20/16.
 */
public interface HBaseGet {
  Result execute() throws IOException;
}
