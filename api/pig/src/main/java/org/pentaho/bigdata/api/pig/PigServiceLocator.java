package org.pentaho.bigdata.api.pig;

import org.pentaho.bigdata.api.configuration.NamedConfiguration;

/**
 * Created by bryan on 6/18/15.
 */
public interface PigServiceLocator {
  PigService create( NamedConfiguration namedConfiguration );
}
