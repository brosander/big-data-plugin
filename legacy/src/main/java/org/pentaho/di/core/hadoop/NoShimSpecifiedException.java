package org.pentaho.di.core.hadoop;

import org.pentaho.hadoop.shim.ConfigurationException;

/**
 * Created by bryan on 8/19/15.
 */
public class NoShimSpecifiedException extends ConfigurationException {
  public NoShimSpecifiedException( String message ) {
    super( message );
  }
}
