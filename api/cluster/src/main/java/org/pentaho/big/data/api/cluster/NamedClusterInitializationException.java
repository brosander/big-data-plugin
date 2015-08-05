package org.pentaho.big.data.api.cluster;

/**
 * Created by bryan on 8/5/15.
 */
public class NamedClusterInitializationException extends Exception {
  public NamedClusterInitializationException( String message ) {
    super( message );
  }

  public NamedClusterInitializationException( Exception e ) {
    super( e );
  }
}
