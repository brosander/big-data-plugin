package org.pentaho.bigdata.api.mapreduce;

import org.pentaho.bigdata.api.configuration.NamedConfiguration;

/**
 * Created by bryan on 6/18/15.
 */
public interface MapReduceServiceLocator {
  MapReduceService create( NamedConfiguration namedConfiguration );
}
