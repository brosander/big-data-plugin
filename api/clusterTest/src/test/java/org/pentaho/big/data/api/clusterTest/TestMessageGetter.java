package org.pentaho.big.data.api.clusterTest;

import org.pentaho.big.data.api.clusterTest.i18n.MessageGetter;
import org.pentaho.di.i18n.BaseMessages;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 8/21/15.
 */
public class TestMessageGetter implements MessageGetter {
  private final Class<?> PKG;

  public TestMessageGetter( Class<?> PKG ) {
    this.PKG = PKG;
  }

  @Override public String getMessage( String key, String... parameters ) {
    StringBuilder stringBuilder = new StringBuilder( "BaseMessages equivalent: BaseMessage.getMessage( " );
    stringBuilder.append( PKG );
    stringBuilder.append( ", \"" );
    stringBuilder.append( key );
    stringBuilder.append( "\"" );
    String realValue;
    boolean hasParameters = parameters != null && parameters.length > 0;
    if ( hasParameters ) {
      realValue = BaseMessages.getString( PKG, key, parameters );
      stringBuilder.append( ", \"" );
      for ( String parameter : parameters ) {
        stringBuilder.append( parameter );
        stringBuilder.append( "\", \"" );
      }
      stringBuilder.setLength( stringBuilder.length() - 3 );
    } else {
      realValue = BaseMessages.getString( PKG, key );
    }
    assertNotEquals( "!" + key + "!", realValue );
    stringBuilder.append( " )" );
    if ( hasParameters ) {
      for ( String parameter : parameters ) {
        assertTrue( "Expected " + realValue + " to contain \"" + parameter + "\"",
          realValue.contains( parameter ) );
      }
    }
    return stringBuilder.toString();
  }
}
