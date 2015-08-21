package org.pentaho.big.data.api.clusterTest;

import org.pentaho.big.data.api.clusterTest.i18n.MessageGetter;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;

/**
 * Created by bryan on 8/21/15.
 */
public class TestMessageGetterFactory implements MessageGetterFactory {
  @Override public MessageGetter create( Class<?> PKG ) {
    return new TestMessageGetter( PKG );
  }
}
