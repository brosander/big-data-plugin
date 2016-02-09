package com.pentaho.big.data.bundles.impl.shim.hbase;

import java.io.IOException;

/**
 * Created by bryan on 2/4/16.
 */
public class IOExceptionUtil {
  public static IOException wrapIfNecessary( Throwable throwable ) {
    if ( throwable instanceof IOException ) {
      return (IOException) throwable;
    }
    return new IOException( throwable );
  }
}
