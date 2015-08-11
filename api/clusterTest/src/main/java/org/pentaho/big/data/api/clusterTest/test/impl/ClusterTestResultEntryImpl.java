package org.pentaho.big.data.api.clusterTest.test.impl;

import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;

/**
 * Created by bryan on 8/12/15.
 */
public class ClusterTestResultEntryImpl implements ClusterTestResultEntry {
  private final ClusterTestEntrySeverity severity;
  private final String description;
  private final String message;
  private final Throwable exception;

  public ClusterTestResultEntryImpl( ClusterTestEntrySeverity severity, String description, String message ) {
    this( severity, description, message, null );
  }

  public ClusterTestResultEntryImpl( ClusterTestEntrySeverity severity, String description, String message,
                                     Throwable exception ) {
    this.severity = severity;
    this.description = description;
    this.message = message;
    this.exception = exception;
  }

  @Override public ClusterTestEntrySeverity getSeverity() {
    return severity;
  }

  @Override public String getDescription() {
    return description;
  }

  @Override public String getMessage() {
    return message;
  }

  @Override public Throwable getException() {
    return exception;
  }
}
