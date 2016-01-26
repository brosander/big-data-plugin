package com.pentaho.big.data.bundles.impl.shim.hbase.table;

import com.pentaho.big.data.bundles.impl.shim.hbase.BatchHBaseConnectionOperation;
import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionOperation;
import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionPool;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceFactoryImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceImpl;
import org.pentaho.bigdata.api.hbase.mapping.ColumnFilter;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.bigdata.api.hbase.table.ResultScanner;
import org.pentaho.bigdata.api.hbase.table.ResultScannerBuilder;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import java.io.IOException;

/**
 * Created by bryan on 1/25/16.
 */
public class ResultScannerBuilderImpl implements ResultScannerBuilder {
  private final HBaseConnectionPool hBaseConnectionPool;
  private final HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory;
  private final HBaseBytesUtilShim hBaseBytesUtilShim;
  private final BatchHBaseConnectionOperation batchHBaseConnectionOperation;
  private int caching = 0;

  public ResultScannerBuilderImpl( HBaseConnectionPool hBaseConnectionPool,
                                   HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory,
                                   HBaseBytesUtilShim hBaseBytesUtilShim, final String tableName,
                                   final byte[] keyLowerBound,
                                   final byte[] keyUpperBound ) {
    this.hBaseConnectionPool = hBaseConnectionPool;
    this.hBaseValueMetaInterfaceFactory = hBaseValueMetaInterfaceFactory;
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
    this.batchHBaseConnectionOperation = new BatchHBaseConnectionOperation();
    batchHBaseConnectionOperation.addOperation( new HBaseConnectionOperation() {
      @Override public void perform( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException {
        try {
          hBaseConnectionWrapper.newSourceTable( tableName );
          hBaseConnectionWrapper.newSourceTableScan( keyLowerBound, keyUpperBound, caching );
        } catch ( Exception e ) {
          throw new IOException( e );
        }
      }
    } );
  }

  @Override
  public void addColumnToScan( final String colFamilyName, final String colName, final boolean colNameIsBinary )
    throws IOException {
    batchHBaseConnectionOperation.addOperation( new HBaseConnectionOperation() {
      @Override public void perform( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException {
        try {
          hBaseConnectionWrapper.addColumnToScan( colFamilyName, colName, colNameIsBinary );
        } catch ( Exception e ) {
          throw new IOException( e );
        }
      }
    } );
  }

  @Override public void addColumnFilterToScan( ColumnFilter cf, HBaseValueMetaInterface columnMeta,
                                               final VariableSpace vars,
                                               final boolean matchAny ) throws IOException {
    final org.pentaho.hbase.shim.api.ColumnFilter columnFilter =
      new org.pentaho.hbase.shim.api.ColumnFilter( cf.getFieldAlias() );
    columnFilter.setFormat( cf.getFormat() );
    columnFilter.setConstant( cf.getConstant() );
    columnFilter.setSignedComparison( cf.getSignedComparison() );
    columnFilter.setFieldType( cf.getFieldType() );
    columnFilter.setComparisonOperator(
      org.pentaho.hbase.shim.api.ColumnFilter.ComparisonType.valueOf( cf.getComparisonOperator().name() ) );
    final HBaseValueMetaInterfaceImpl hBaseValueMetaInterface = hBaseValueMetaInterfaceFactory.copy( columnMeta );
    batchHBaseConnectionOperation.addOperation( new HBaseConnectionOperation() {
      @Override public void perform( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException {
        try {
          hBaseConnectionWrapper
            .addColumnFilterToScan( columnFilter, hBaseValueMetaInterface, vars, matchAny );
        } catch ( Exception e ) {
          throw new IOException( e );
        }
      }
    } );
  }

  @Override public void setCaching( int cacheSize ) {
    this.caching = cacheSize;
  }

  @Override public ResultScanner build() throws IOException {
    HBaseConnectionHandle connectionHandle = hBaseConnectionPool.getConnectionHandle();
    batchHBaseConnectionOperation.perform( connectionHandle.getConnection() );
    return new ResultScannerImpl( connectionHandle, hBaseBytesUtilShim );
  }
}
