package org.pentaho.big.data.api.clusterTest.test;

/**
 * Created by bryan on 8/11/15.
 */
public interface ClusterTestResultEntry {
  ClusterTestEntrySeverity getSeverity();
  String getDescription();
  String getMessage();
  Throwable getException();
}
