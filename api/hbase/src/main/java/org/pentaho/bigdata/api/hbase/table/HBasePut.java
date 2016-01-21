package org.pentaho.bigdata.api.hbase.table;

import java.io.IOException;

/**
 * Created by bryan on 1/20/16.
 */
public interface HBasePut {
  void setWriteToWAL( boolean writeToWAL );

  void addColumn( String columnFamily, String columnName, boolean colNameIsBinary, byte[] colValue ) throws
    IOException;

  String createColumnName( String... parts );

  void execute() throws IOException;
}
