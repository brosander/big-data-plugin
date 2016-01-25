package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hbase.shim.api.ColumnFilter;
import org.pentaho.hbase.shim.api.HBaseValueMeta;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;
import org.pentaho.hbase.shim.spi.HBaseConnection;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.NavigableMap;
import java.util.Properties;

/**
 * Created by bryan on 1/25/16.
 */
public class HBaseConnectionWrapper extends HBaseConnection {
  private final HBaseConnection delegate;
  private final HBaseConnection realImpl;
  private final Field resultSetRowField;

  public HBaseConnectionWrapper( HBaseConnection delegate ) {
    this.delegate = delegate;
    this.realImpl = findRealImpl( delegate );
    resultSetRowField = getResultSetRowField( this.realImpl );
    resultSetRowField.setAccessible( true );
  }

  @Override public HBaseBytesUtilShim getBytesUtil() throws Exception {
    return delegate.getBytesUtil();
  }

  @Override public void configureConnection( Properties properties, List<String> list ) throws Exception {
    delegate.configureConnection( properties, list );
  }

  @Override public void checkHBaseAvailable() throws Exception {
    delegate.checkHBaseAvailable();
  }

  @Override public List<String> listTableNames() throws Exception {
    return delegate.listTableNames();
  }

  @Override public boolean tableExists( String s ) throws Exception {
    return delegate.tableExists( s );
  }

  @Override public boolean isTableDisabled( String s ) throws Exception {
    return delegate.isTableDisabled( s );
  }

  @Override public boolean isTableAvailable( String s ) throws Exception {
    return delegate.isTableAvailable( s );
  }

  @Override public void disableTable( String s ) throws Exception {
    delegate.disableTable( s );
  }

  @Override public void enableTable( String s ) throws Exception {
    delegate.enableTable( s );
  }

  @Override public void deleteTable( String s ) throws Exception {
    delegate.deleteTable( s );
  }

  @Override public void executeTargetTableDelete( byte[] bytes ) throws Exception {
    delegate.executeTargetTableDelete( bytes );
  }

  @Override public void createTable( String s, List<String> list, Properties properties ) throws Exception {
    delegate.createTable( s, list, properties );
  }

  @Override public List<String> getTableFamiles( String s ) throws Exception {
    return delegate.getTableFamiles( s );
  }

  @Override public void newSourceTable( String s ) throws Exception {
    delegate.newSourceTable( s );
  }

  @Override public boolean sourceTableRowExists( byte[] bytes ) throws Exception {
    return delegate.sourceTableRowExists( bytes );
  }

  @Override public void newSourceTableScan( byte[] bytes, byte[] bytes1, int i ) throws Exception {
    delegate.newSourceTableScan( bytes, bytes1, i );
  }

  @Override public void newTargetTablePut( byte[] bytes, boolean b ) throws Exception {
    delegate.newTargetTablePut( bytes, b );
  }

  @Override public boolean targetTableIsAutoFlush() throws Exception {
    return delegate.targetTableIsAutoFlush();
  }

  @Override public void executeTargetTablePut() throws Exception {
    delegate.executeTargetTablePut();
  }

  @Override public void flushCommitsTargetTable() throws Exception {
    delegate.flushCommitsTargetTable();
  }

  @Override public void addColumnToTargetPut( String s, String s1, boolean b, byte[] bytes ) throws Exception {
    delegate.addColumnToTargetPut( s, s1, b, bytes );
  }

  @Override public void addColumnFilterToScan( ColumnFilter columnFilter,
                                               HBaseValueMeta hBaseValueMeta,
                                               VariableSpace variableSpace, boolean b ) throws Exception {
    delegate.addColumnFilterToScan( columnFilter, hBaseValueMeta, variableSpace, b );
  }

  @Override public void addColumnToScan( String s, String s1, boolean b ) throws Exception {
    delegate.addColumnToScan( s, s1, b );
  }

  @Override public void executeSourceTableScan() throws Exception {
    delegate.executeSourceTableScan();
  }

  @Override public boolean resultSetNextRow() throws Exception {
    return delegate.resultSetNextRow();
  }

  @Override public byte[] getRowKey( Object o ) throws Exception {
    return delegate.getRowKey( o );
  }

  @Override public byte[] getResultSetCurrentRowKey() throws Exception {
    return delegate.getResultSetCurrentRowKey();
  }

  @Override public byte[] getRowColumnLatest( Object o, String s, String s1, boolean b ) throws Exception {
    return delegate.getRowColumnLatest( o, s, s1, b );
  }

  @Override public boolean checkForHBaseRow( Object o ) {
    return delegate.checkForHBaseRow( o );
  }

  @Override public byte[] getResultSetCurrentRowColumnLatest( String s, String s1, boolean b ) throws Exception {
    return delegate.getResultSetCurrentRowColumnLatest( s, s1, b );
  }

  @Override public NavigableMap<byte[], byte[]> getRowFamilyMap( Object o, String s ) throws Exception {
    return delegate.getRowFamilyMap( o, s );
  }

  @Override public NavigableMap<byte[], byte[]> getResultSetCurrentRowFamilyMap( String s ) throws Exception {
    return delegate.getResultSetCurrentRowFamilyMap( s );
  }

  @Override public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getRowMap(
    Object o ) throws Exception {
    return delegate.getRowMap( o );
  }

  @Override public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getResultSetCurrentRowMap()
    throws Exception {
    return delegate.getResultSetCurrentRowMap();
  }

  @Override public void closeSourceTable() throws Exception {
    delegate.closeSourceTable();
  }

  @Override public void closeSourceResultSet() throws Exception {
    delegate.closeSourceResultSet();
  }

  @Override public void newTargetTable( String s, Properties properties ) throws Exception {
    delegate.newTargetTable( s, properties );
  }

  @Override public void closeTargetTable() throws Exception {
    delegate.closeTargetTable();
  }

  @Override public boolean isImmutableBytesWritable( Object o ) {
    return delegate.isImmutableBytesWritable( o );
  }

  @Override public void close() throws Exception {
    delegate.close();
  }

  public Result getCurrentResult() {
    try {
      return (Result) resultSetRowField.get( realImpl );
    } catch ( IllegalAccessException e ) {
      return null;
    }
  }

  private Field getResultSetRowField( Object o ) {
    try {
      return o.getClass().getDeclaredField( "m_currentResultSetRow" );
    } catch ( NoSuchFieldException e ) {
      return null;
    }
  }

  private HBaseConnection findRealImpl( Object hBaseConnection ) {
    if ( Proxy.isProxyClass( hBaseConnection.getClass() ) ) {
      return  unwrapProxy( hBaseConnection );
    } else if ( hBaseConnection instanceof HBaseConnection ) {
      if ( getResultSetRowField( hBaseConnection ) != null ) {
        return (HBaseConnection) hBaseConnection;
      }
      try {
        Object delegate = getFieldValue( hBaseConnection.getClass().getDeclaredField( "delegate" ), hBaseConnection );
        if ( delegate instanceof HBaseConnection ) {
          return findRealImpl( delegate );
        }
      } catch ( NoSuchFieldException e ) {
        e.printStackTrace();
      }
    }
    return null;
  }

  private Object getFieldValue( Field field, Object object ) {
    final boolean setAccessible = !field.isAccessible();
    try {
      if ( setAccessible ) {
        field.setAccessible( true );
      }
      return field.get( object );
    } catch ( IllegalAccessException e ) {
      e.printStackTrace();
    } finally {
      if ( setAccessible ) {
        field.setAccessible( false );
      }
    }
    return null;
  }

  private HBaseConnection unwrapProxy( Object proxy ) {
    InvocationHandler invocationHandler = Proxy.getInvocationHandler( proxy );
    for ( Field field : invocationHandler.getClass().getDeclaredFields() ) {
      Object value = getFieldValue( field, proxy );
      if ( value instanceof HBaseConnection ) {
        return findRealImpl( value );
      }
    }
    return null;
  }
}
