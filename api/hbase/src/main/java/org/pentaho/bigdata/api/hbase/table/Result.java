package org.pentaho.bigdata.api.hbase.table;

import java.io.IOException;
import java.util.NavigableMap;

/**
 * Created by bryan on 1/19/16.
 */
public interface Result {
  byte[] getRow() throws IOException;

  NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getMap() throws IOException;

  NavigableMap<byte[], byte[]> getFamilyMap( String familyName ) throws IOException;

  byte[] getValue( String colFamilyName, String colName, boolean colNameIsBinary ) throws IOException;

  boolean isEmpty() throws IOException;
}
