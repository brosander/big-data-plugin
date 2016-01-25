package com.pentaho.big.data.bundles.impl.shim.hbase.table;

import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionPool;
import org.pentaho.bigdata.api.hbase.mapping.ColumnFilter;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.bigdata.api.hbase.table.ResultScanner;
import org.pentaho.bigdata.api.hbase.table.ResultScannerBuilder;
import org.pentaho.di.core.variables.VariableSpace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bryan on 1/25/16.
 */
public class ResultScannerBuilderImpl implements ResultScannerBuilder {
  private final HBaseConnectionPool hBaseConnectionPool;
  private final String tableName;
  private final List<Operation> operations;
  private final byte[] keyLowerBound;
  private final byte[] keyUpperBound;
  private int caching = 0;

  public ResultScannerBuilderImpl( HBaseConnectionPool hBaseConnectionPool, String tableName, final byte[] keyLowerBound,
                                   final byte[] keyUpperBound ) {
    this.hBaseConnectionPool = hBaseConnectionPool;
    this.tableName = tableName;
    this.keyLowerBound = keyLowerBound;
    this.keyUpperBound = keyUpperBound;
    this.operations = new ArrayList<>();
    operations.add( new Operation() {
      @Override public void add( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException {
        try {
          hBaseConnectionWrapper.newSourceTableScan( keyLowerBound, keyUpperBound, caching );
        } catch ( Exception e ) {
          throw new IOException( e );
        }
      }
    } );
  }

  @Override public void addColumnToScan( final String colFamilyName, final String colName, final boolean colNameIsBinary )
    throws IOException {
    operations.add( new Operation() {
      @Override public void add( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException {
        try {
          hBaseConnectionWrapper.addColumnToScan( colFamilyName, colName, colNameIsBinary );
        } catch ( Exception e ) {
          throw new IOException( e );
        }
      }
    } );
  }

  @Override public void addColumnFilterToScan( final ColumnFilter cf, final HBaseValueMetaInterface columnMeta, final VariableSpace vars,
                                               final boolean matchAny ) throws IOException {
    operations.add( new Operation() {
      @Override public void add( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException {
        hBaseConnectionWrapper.addColumnFilterToScan( org.pentaho.hbase.shim.api.ColumnFilter., columnMeta, vars, matchAny );
      }
    } );
  }

  @Override public void setCaching( int cacheSize ) {

  }

  @Override public ResultScanner build() throws IOException {
    return null;
  }

  interface Operation {
    void add( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException;
  }
}
