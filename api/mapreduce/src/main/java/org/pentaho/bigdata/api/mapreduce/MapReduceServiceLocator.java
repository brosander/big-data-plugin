package org.pentaho.bigdata.api.mapreduce;

import org.pentaho.big.data.api.cluster.NamedCluster;

/**
 * Created by bryan on 6/18/15.
 */
public interface MapReduceServiceLocator {
  MapReduceService create( NamedCluster namedConfiguration );
}
