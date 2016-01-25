package com.pentaho.big.data.bundles.impl.shim.hbase.table;

import org.pentaho.bigdata.api.hbase.table.Result;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import java.io.IOException;
import java.util.NavigableMap;

/**
 * Created by bryan on 1/22/16.
 */
public class ResultImpl implements Result {
  private final org.apache.hadoop.hbase.client.Result result;
  private final HBaseBytesUtilShim hBaseBytesUtilShim;

  public ResultImpl( org.apache.hadoop.hbase.client.Result result, HBaseBytesUtilShim hBaseBytesUtilShim ) {
    this.result = result;
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
  }

  @Override public byte[] getRow() {
    return result.getRow();
  }

  @Override public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getMap() {
    return result.getMap();
  }

  @Override public NavigableMap<byte[], byte[]> getFamilyMap( String familyName ) {
    return result.getFamilyMap( hBaseBytesUtilShim.toBytes( familyName ) );
  }

  @Override public byte[] getValue( String colFamilyName, String colName, boolean colNameIsBinary ) {
    return result.getValue( hBaseBytesUtilShim.toBytes( colFamilyName ),
      colNameIsBinary ? hBaseBytesUtilShim.toBytesBinary( colName ) : hBaseBytesUtilShim.toBytes( colName ) );
  }

  @Override public boolean isEmpty() {
    return result.isEmpty();
  }
}
