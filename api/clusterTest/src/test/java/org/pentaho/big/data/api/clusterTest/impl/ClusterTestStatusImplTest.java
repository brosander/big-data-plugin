package org.pentaho.big.data.api.clusterTest.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.clusterTest.module.ClusterTestModuleResults;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 8/20/15.
 */
public class ClusterTestStatusImplTest {
  private List<ClusterTestModuleResults> clusterTestModuleResults;
  private boolean done;
  private ClusterTestStatusImpl clusterTestStatus;

  @Before
  public void setup() {
    clusterTestModuleResults = mock( List.class );
    done = true;
    initStatus();
  }

  private void initStatus() {
    clusterTestStatus = new ClusterTestStatusImpl( clusterTestModuleResults, done );
  }

  @Test
  public void testConstructor() {
    assertEquals( clusterTestModuleResults, clusterTestStatus.getModuleResults() );
    assertTrue( clusterTestStatus.isDone() );
    done = false;
    initStatus();
    assertEquals( clusterTestModuleResults, clusterTestStatus.getModuleResults() );
    assertFalse( clusterTestStatus.isDone() );
  }

  @Test
  public void testToString() {
    assertTrue( clusterTestStatus.toString().contains( clusterTestModuleResults.toString() ) );
    assertTrue( clusterTestStatus.toString().contains( "done=true" ) );
    done = false;
    initStatus();
    assertTrue( clusterTestStatus.toString().contains( "done=false" ) );
  }
}
