package org.pentaho.big.data.api.clusterTest.test.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.clusterTest.test.ClusterTest;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/20/15.
 */
public class ClusterTestResultImplTest {
  private ClusterTest clusterTest;
  private List<ClusterTestResultEntry> clusterTestResultEntries;
  private long timeTaken;
  private ClusterTestResultImpl clusterTestResult;
  private ClusterTestResultEntry clusterTestResultEntry;
  private ClusterTestEntrySeverity info;

  @Before
  public void setup() {
    clusterTest = mock( ClusterTest.class );
    clusterTestResultEntry = mock( ClusterTestResultEntry.class );
    info = ClusterTestEntrySeverity.INFO;
    when( clusterTestResultEntry.getSeverity() ).thenReturn( info );
    clusterTestResultEntries = new ArrayList<>( Arrays.asList( clusterTestResultEntry ) );
    timeTaken = 10L;
    clusterTestResult = new ClusterTestResultImpl( clusterTest, clusterTestResultEntries, timeTaken );
  }

  @Test
  public void testGetMaxSeverity() {
    assertEquals( info, clusterTestResult.getMaxSeverity() );
  }

  @Test
  public void testGetClusterTestResultEntries() {
    assertEquals( clusterTestResultEntries, clusterTestResult.getClusterTestResultEntries() );
  }

  @Test
  public void testGetClusterTest() {
    assertEquals( clusterTest, clusterTestResult.getClusterTest() );
  }

  @Test
  public void testGetTimeTaken() {
    assertEquals( timeTaken, clusterTestResult.getTimeTaken() );
  }

  @Test
  public void testToString() {
    String string = clusterTestResult.toString();
    assertTrue( string.contains( info.toString() ) );
    assertTrue( string.contains( clusterTestResultEntry.toString() ) );
    assertTrue( string.contains( clusterTest.toString() ) );
    assertTrue( string.contains( String.valueOf( timeTaken ) ) );
  }
}
