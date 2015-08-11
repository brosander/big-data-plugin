package org.pentaho.bigdata.api.hdfs.exceptions;

import java.io.IOException;

/**
 * Created by bryan on 8/19/15.
 */
public class AccessControlException extends IOException {
  public AccessControlException( String message, Throwable cause ) {
    super( message, cause );
  }
}
