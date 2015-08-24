package org.pentaho.big.data.api.clusterTest.network.impl;

import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.network.ConnectivityTest;
import org.pentaho.big.data.api.clusterTest.network.ConnectivityTestFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;

/**
 * Created by bryan on 8/24/15.
 */
public class ConnectivityTestFactoryImpl implements ConnectivityTestFactory {
  @Override public ConnectivityTest create( MessageGetterFactory messageGetterFactory, String hostname, String port,
                                            boolean haPossible ) {
    return create( messageGetterFactory, hostname, port, haPossible, ClusterTestEntrySeverity.FATAL );
  }

  @Override public ConnectivityTest create( MessageGetterFactory messageGetterFactory, String hostname, String port,
                                            boolean haPossible, ClusterTestEntrySeverity severityOfFailures ) {
    return new ConnectivityTestImpl( messageGetterFactory, hostname, port, haPossible, severityOfFailures );
  }
}
