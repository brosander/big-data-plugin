package org.pentaho.bigdata.api.pig;

import org.pentaho.big.data.api.cluster.NamedCluster;

/**
 * Created by bryan on 6/18/15.
 */
public interface PigServiceFactory {
  PigService create( NamedCluster namedCluster );
}
