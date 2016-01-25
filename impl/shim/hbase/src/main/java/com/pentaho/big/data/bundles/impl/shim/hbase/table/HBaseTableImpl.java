package com.pentaho.big.data.bundles.impl.shim.hbase.table;

import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionPool;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.bigdata.api.hbase.table.HBaseDelete;
import org.pentaho.bigdata.api.hbase.table.HBaseGet;
import org.pentaho.bigdata.api.hbase.table.HBasePut;
import org.pentaho.bigdata.api.hbase.table.HBaseTable;
import org.pentaho.bigdata.api.hbase.table.ResultScannerBuilder;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Created by bryan on 1/22/16.
 */
public class HBaseTableImpl implements HBaseTable {
  private final HBaseConnectionPool hBaseConnectionPool;
  private final String name;

  public HBaseTableImpl( HBaseConnectionPool hBaseConnectionPool, String name ) {
    this.hBaseConnectionPool = hBaseConnectionPool;
    this.name = name;
  }

  @Override public boolean exists() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      return hBaseConnectionHandle.getConnection().tableExists( name );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public boolean disabled() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      return hBaseConnectionHandle.getConnection().isTableDisabled( name );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public boolean available() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      return hBaseConnectionHandle.getConnection().isTableAvailable( name );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public void disable() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      hBaseConnectionHandle.getConnection().disableTable( name );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public void enable() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      hBaseConnectionHandle.getConnection().enableTable( name );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public void delete() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      hBaseConnectionHandle.getConnection().deleteTable( name );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public void create( List<String> colFamilyNames, Properties creationProps ) throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      hBaseConnectionHandle.getConnection().createTable( name, colFamilyNames, creationProps );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public ResultScannerBuilder createScannerBuilder( byte[] keyLowerBound, byte[] keyUpperBound ) {
    return null;
  }

  @Override
  public ResultScannerBuilder createScannerBuilder( Mapping tableMapping, String dateOrNumberConversionMaskForKey,
                                                    String keyStartS, String keyStopS, String scannerCacheSize,
                                                    LogChannelInterface log, VariableSpace vars ) {
    return null;
  }

  @Override public void setWriteBufferSize( long value ) {

  }

  @Override public boolean isAutoFlush() {
    return false;
  }

  @Override public void setAutoFlush( boolean value ) {

  }

  @Override public HBasePut createPut( byte[] key ) {
    return null;
  }

  @Override public HBaseGet createGet( byte[] key ) {
    return null;
  }

  @Override public HBaseDelete createDelete( byte[] key ) {
    return null;
  }

  @Override public List<String> getColumnFamilies() throws IOException {
    return null;
  }

  @Override public void flushCommits() throws IOException {

  }

  @Override public void close() throws IOException {

  }
}
