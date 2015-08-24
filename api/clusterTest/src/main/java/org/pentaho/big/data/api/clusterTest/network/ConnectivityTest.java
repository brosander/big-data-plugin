package org.pentaho.big.data.api.clusterTest.network;

import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;

import java.util.List;

/**
 * Created by bryan on 8/24/15.
 */
public interface ConnectivityTest {
  List<ClusterTestResultEntry> runTest();
}
