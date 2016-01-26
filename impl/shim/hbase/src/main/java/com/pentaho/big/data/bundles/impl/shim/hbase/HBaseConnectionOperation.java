package com.pentaho.big.data.bundles.impl.shim.hbase;

import java.io.IOException;

/**
 * Created by bryan on 1/26/16.
 */
public interface HBaseConnectionOperation {
  void perform( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException;
}
