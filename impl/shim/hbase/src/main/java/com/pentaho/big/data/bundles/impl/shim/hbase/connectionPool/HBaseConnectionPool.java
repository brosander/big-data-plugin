package com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool;

import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.hbase.shim.spi.HBaseConnection;
import org.pentaho.hbase.shim.spi.HBaseShim;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Created by bryan on 1/25/16.
 */
public class HBaseConnectionPool implements Closeable {
  private final Set<HBaseConnectionHandle> availableConnections;
  private final Set<HBaseConnectionHandle> inUseConnections;
  private final HBaseShim hBaseShim;
  private final Properties connectionProps;
  private final LogChannelInterface logChannelInterface;

  public HBaseConnectionPool( HBaseShim hBaseShim, Properties connectionProps,
                              LogChannelInterface logChannelInterface ) {
    this.hBaseShim = hBaseShim;
    this.connectionProps = connectionProps;
    this.logChannelInterface = logChannelInterface;
    availableConnections = new HashSet<>();
    inUseConnections = new HashSet<>();
  }

  public synchronized HBaseConnectionHandle getConnectionHandle() throws IOException {
    if ( availableConnections.size() > 0 ) {
      final HBaseConnectionHandle hBaseConnectionHandle = availableConnections.iterator().next();
      availableConnections.remove( hBaseConnectionHandle );
      inUseConnections.add( hBaseConnectionHandle );
      return hBaseConnectionHandle;
    }

    final HBaseConnection hBaseConnection = hBaseShim.getHBaseConnection();
    try {
      hBaseConnection.configureConnection( connectionProps, new ArrayList<String>() );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
    HBaseConnectionPoolConnection hBaseConnectionPoolConnection = new HBaseConnectionPoolConnection( hBaseConnection );
    HBaseConnectionHandleImpl hBaseConnectionHandle = new HBaseConnectionHandleImpl( this, hBaseConnectionPoolConnection );
    hBaseConnectionPoolConnection.init( hBaseConnectionHandle );
    inUseConnections.add( hBaseConnectionHandle );
    return hBaseConnectionHandle;
  }

  protected synchronized void releaseConnection( HBaseConnectionHandle hBaseConnectionHandle ) {
    inUseConnections.remove( hBaseConnectionHandle );
    availableConnections.add( hBaseConnectionHandle );
  }

  @Override public synchronized void close() throws IOException {
    for ( HBaseConnectionHandle inUseConnection : inUseConnections ) {
      try {
        inUseConnection.getConnection().close();
      } catch ( Exception e ) {
        logChannelInterface.logError( e.getMessage(), e );
      }
    }
    for ( HBaseConnectionHandle inUseConnection : availableConnections ) {
      try {
        inUseConnection.getConnection().close();
      } catch ( Exception e ) {
        logChannelInterface.logError( e.getMessage(), e );
      }
    }
    inUseConnections.clear();
    availableConnections.clear();
  }
}
