package com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool;

import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;
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
  private final Set<HBaseConnectionWrapper> availableConnections;
  private final Set<HBaseConnectionWrapper> inUseConnections;
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
      final HBaseConnectionWrapper hBaseConnectionWrapper = availableConnections.iterator().next();
      availableConnections.remove( hBaseConnectionWrapper );
      HBaseConnectionHandleImpl hBaseConnectionHandle = new HBaseConnectionHandleImpl( this, hBaseConnectionWrapper );
      inUseConnections.add( hBaseConnectionWrapper );
      return hBaseConnectionHandle;
    }

    final HBaseConnection hBaseConnection = hBaseShim.getHBaseConnection();
    try {
      hBaseConnection.configureConnection( connectionProps, new ArrayList<String>() );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
    HBaseConnectionWrapper hBaseConnectionPoolConnection = new HBaseConnectionWrapper( hBaseConnection );
    HBaseConnectionHandleImpl hBaseConnectionHandle =
      new HBaseConnectionHandleImpl( this, hBaseConnectionPoolConnection );
    inUseConnections.add( hBaseConnectionPoolConnection );
    return hBaseConnectionHandle;
  }

  protected synchronized void releaseConnection( HBaseConnectionWrapper hBaseConnection ) {
    inUseConnections.remove( hBaseConnection );
    availableConnections.add( hBaseConnection );
  }

  @Override public synchronized void close() throws IOException {
    for ( HBaseConnectionWrapper inUseConnection : inUseConnections ) {
      try {
        inUseConnection.close();
      } catch ( Exception e ) {
        logChannelInterface.logError( e.getMessage(), e );
      }
    }
    for ( HBaseConnectionWrapper availableConnection : availableConnections ) {
      try {
        availableConnection.close();
      } catch ( Exception e ) {
        logChannelInterface.logError( e.getMessage(), e );
      }
    }
    inUseConnections.clear();
    availableConnections.clear();
  }
}
