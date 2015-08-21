package org.pentaho.big.data.api.clusterTest.test.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 8/21/15.
 */
public class ClusterTestResultEntryImplTest {

  private ClusterTestEntrySeverity severity;
  private String description;
  private String message;
  private Exception exception;
  private ClusterTestResultEntryImpl clusterTestResultEntry;

  @Before
  public void setup() {
    severity = ClusterTestEntrySeverity.ERROR;
    description = "desc";
    message = "msg";
    exception = new Exception();
    clusterTestResultEntry = new ClusterTestResultEntryImpl( severity, description, message, exception );
  }

  @Test
  public void test3ArgConstructor() {
    exception = null;
    clusterTestResultEntry = new ClusterTestResultEntryImpl( severity, description, message );
    testGetSeverity();
    testGetDescription();
    testGetMessage();
    testToString();
  }

  @Test
  public void testGetSeverity() {
    assertEquals( severity, clusterTestResultEntry.getSeverity() );
  }

  @Test
  public void testGetDescription() {
    assertEquals( description, clusterTestResultEntry.getDescription() );
  }

  @Test
  public void testGetMessage() {
    assertEquals( message, clusterTestResultEntry.getMessage() );
  }

  @Test
  public void testGetException() {
    assertEquals( exception, clusterTestResultEntry.getException() );
  }

  @Test
  public void testToString() {
    String string = clusterTestResultEntry.toString();
    assertTrue( string.contains( severity.toString() ) );
    assertTrue( string.contains( description ) );
    assertTrue( string.contains( message ) );
    assertTrue( string.contains( String.valueOf( exception ) ) );
  }
}
