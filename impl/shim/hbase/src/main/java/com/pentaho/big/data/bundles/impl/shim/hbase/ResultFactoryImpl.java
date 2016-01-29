package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.pentaho.bigdata.api.hbase.Result;
import org.pentaho.bigdata.api.hbase.ResultFactory;
import org.pentaho.bigdata.api.hbase.ResultFactoryException;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

/**
 * Created by bryan on 1/29/16.
 */
public class ResultFactoryImpl implements ResultFactory {
  private final HBaseBytesUtilShim hBaseBytesUtilShim;

  public ResultFactoryImpl( HBaseBytesUtilShim hBaseBytesUtilShim ) {
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
  }

  @Override public boolean canHandle( Object object ) {
    return object == null || object instanceof org.apache.hadoop.hbase.client.Result;
  }

  @Override public Result create( Object object ) throws ResultFactoryException {
    if ( object == null ) {
      return null;
    }
    try {
      return new ResultImpl( (org.apache.hadoop.hbase.client.Result) object, hBaseBytesUtilShim );
    } catch ( ClassCastException e ) {
      throw new ResultFactoryException( e );
    }
  }
}
