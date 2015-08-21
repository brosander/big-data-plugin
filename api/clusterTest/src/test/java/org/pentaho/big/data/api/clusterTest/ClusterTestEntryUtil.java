package org.pentaho.big.data.api.clusterTest;

import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by bryan on 8/21/15.
 */
public class ClusterTestEntryUtil {
  public static ClusterTestResultEntry expectOneEntry( List<ClusterTestResultEntry> clusterTestResultEntries ) {
    assertNotNull( clusterTestResultEntries );
    assertEquals( 1, clusterTestResultEntries.size() );
    return clusterTestResultEntries.get( 0 );
  }

  public static void verifyClusterTestResultEntry( ClusterTestResultEntry clusterTestResultEntry,
                                                   ClusterTestEntrySeverity severity, String desc, String message ) {
    verifyClusterTestResultEntry( clusterTestResultEntry, severity, desc, message, null );
  }

  public static Throwable verifyClusterTestResultEntry( ClusterTestResultEntry clusterTestResultEntry,
                                                        ClusterTestEntrySeverity severity, String desc, String message,
                                                        Class<?> exceptionClass ) {
    assertNotNull( clusterTestResultEntry );
    assertEquals( severity, clusterTestResultEntry.getSeverity() );
    assertEquals( desc, clusterTestResultEntry.getDescription() );
    assertEquals( message, clusterTestResultEntry.getMessage() );
    Throwable clusterTestResultEntryException = clusterTestResultEntry.getException();
    if ( exceptionClass == null ) {
      assertNull( clusterTestResultEntryException );
    } else {
      assertTrue( "expected exception of type " + exceptionClass,
        exceptionClass.isInstance( clusterTestResultEntryException ) );
    }
    return clusterTestResultEntryException;
  }
}
