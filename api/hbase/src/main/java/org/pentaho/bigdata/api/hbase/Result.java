package org.pentaho.bigdata.api.hbase;

import java.util.NavigableMap;

/**
 * Created by bryan on 1/19/16.
 */
public interface Result {
  byte[] getRow();

  NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getMap();

  NavigableMap<byte[], byte[]> getFamilyMap( String familyName );

  byte[] getValue( String colFamilyName, String colName, boolean colNameIsBinary );

  boolean isEmpty();
}
