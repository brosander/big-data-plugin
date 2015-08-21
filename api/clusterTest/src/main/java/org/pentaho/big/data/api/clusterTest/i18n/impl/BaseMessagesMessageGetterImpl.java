package org.pentaho.big.data.api.clusterTest.i18n.impl;

import org.pentaho.big.data.api.clusterTest.i18n.MessageGetter;
import org.pentaho.di.i18n.BaseMessages;

/**
 * Created by bryan on 8/21/15.
 */
public class BaseMessagesMessageGetterImpl implements MessageGetter {
  private final Class<?> PKG;

  public BaseMessagesMessageGetterImpl( Class<?> PKG ) {
    this.PKG = PKG;
  }

  @Override public String getMessage( String key, String... parameters ) {
    if ( parameters != null && parameters.length > 0 ) {
      return BaseMessages.getString( PKG, key, parameters );
    } else {
      return BaseMessages.getString( PKG, key );
    }
  }
}
