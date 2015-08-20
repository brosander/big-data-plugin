package org.pentaho.big.data.api.clusterTest.module.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.clusterTest.test.ClusterTest;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/20/15.
 */
public class ClusterTestModuleResultsImplTest {
  private String name;
  private List<ClusterTestResult> clusterTestResults;
  private Set<ClusterTest> runningTests;
  private Set<ClusterTest> outstandingTests;
  private ClusterTestModuleResultsImpl clusterTestModuleResults;
  private ClusterTestResult clusterTestResult;
  private ClusterTest runningTest;
  private ClusterTest outstandingTest;
  private ClusterTestEntrySeverity maxSeverity;

  @Before
  public void setup() {
    name = "testName";
    clusterTestResult = mock( ClusterTestResult.class );
    maxSeverity = ClusterTestEntrySeverity.INFO;
    when( clusterTestResult.getMaxSeverity() ).thenReturn( maxSeverity );
    clusterTestResults = new ArrayList<>( Arrays.asList( clusterTestResult ) );
    runningTest = mock( ClusterTest.class );
    runningTests = new HashSet<>( Arrays.asList( runningTest ) );
    outstandingTest = mock( ClusterTest.class );
    outstandingTests = new HashSet<>( Arrays.asList( outstandingTest ) );
    clusterTestModuleResults =
      new ClusterTestModuleResultsImpl( name, clusterTestResults, runningTests, outstandingTests );
  }

  @Test
  public void testName() {
    assertEquals( name, clusterTestModuleResults.getName() );
  }

  @Test
  public void testGetClusterTestResults() {
    assertEquals( clusterTestResults, clusterTestModuleResults.getClusterTestResults() );
  }

  @Test
  public void testGetRunningTests() {
    assertEquals( runningTests, clusterTestModuleResults.getRunningTests() );
  }

  @Test
  public void testGetOutstandingTests() {
    assertEquals( outstandingTests, clusterTestModuleResults.getOutstandingTests() );
  }

  @Test
  public void testGetMaxSeverity() {
    assertEquals( maxSeverity, clusterTestModuleResults.getMaxSeverity() );
  }

  @Test
  public void testToString() {
    String string = clusterTestModuleResults.toString();
    assertTrue( string.contains( name ) );
    assertTrue( string.contains( clusterTestResult.toString() ) );
    assertTrue( string.contains( runningTest.toString() ) );
    assertTrue( string.contains( outstandingTest.toString() ) );
    assertTrue( string.contains( maxSeverity.toString() ) );
  }
}
