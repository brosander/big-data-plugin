package org.pentaho.big.data.api.clusterTest.network;

import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;

/**
 * Created by bryan on 8/24/15.
 */
public interface ConnectivityTestFactory {
  ConnectivityTest create( MessageGetterFactory messageGetterFactory, String hostname, String port,
                           boolean haPossible );

  ConnectivityTest create( MessageGetterFactory messageGetterFactory, String hostname, String port, boolean haPossible,
                           ClusterTestEntrySeverity severityOfFailures );
}
