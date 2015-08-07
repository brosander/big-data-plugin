package org.pentaho.big.data.impl.vfs.hdfs;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by bryan on 8/7/15.
 */
public class HDFSFileNameParserTest {
  @Test
  public void testDefaultPort() {
    assertEquals( -1, HDFSFileNameParser.getInstance().getDefaultPort() );
  }
}
