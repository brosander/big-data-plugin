package com.pentaho.big.data.bundles.impl.shim.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bryan on 1/26/16.
 */
public class BatchHBaseConnectionOperation implements HBaseConnectionOperation {
  private final List<HBaseConnectionOperation> hBaseConnectionOperations;

  public BatchHBaseConnectionOperation() {
    hBaseConnectionOperations = new ArrayList<>();
  }

  public void addOperation( HBaseConnectionOperation hBaseConnectionOperation ) {
    hBaseConnectionOperations.add( hBaseConnectionOperation );
  }

  @Override public void perform( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException {
    for ( HBaseConnectionOperation hBaseConnectionOperation : hBaseConnectionOperations ) {
      hBaseConnectionOperation.perform( hBaseConnectionWrapper );
    }
  }
}
