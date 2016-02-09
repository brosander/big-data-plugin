package com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by bryan on 2/4/16.
 */
public class HBaseConnectionHandleImplTest {
  private HBaseConnectionPool hBaseConnectionPool;
  private HBaseConnectionPoolConnection hBaseConnection;
  private HBaseConnectionHandleImpl hBaseConnectionHandle;

  @Before
  public void setup() {
    hBaseConnectionPool = mock( HBaseConnectionPool.class );
    hBaseConnection = mock( HBaseConnectionPoolConnection.class );
    hBaseConnectionHandle = new HBaseConnectionHandleImpl( hBaseConnectionPool, hBaseConnection );
  }

  @Test
  public void testGetConnection() {
    assertEquals( hBaseConnection, hBaseConnectionHandle.getConnection() );
  }

  @Test
  public void testClose() throws IOException {
    hBaseConnectionHandle.close();
    assertNull( hBaseConnectionHandle.getConnection() );
    verify( hBaseConnectionPool ).releaseConnection( hBaseConnection );
  }
}
