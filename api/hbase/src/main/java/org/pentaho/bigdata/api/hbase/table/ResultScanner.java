package org.pentaho.bigdata.api.hbase.table;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by bryan on 1/19/16.
 */
public interface ResultScanner extends Closeable {
  Result next() throws IOException;
}