package org.pentaho.bigdata.kettle.plugins.common;

import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;

/**
 * Created by bryan on 6/19/15.
 */
public class BigDataXmlHelperTest {
  private BigDataXmlHelper bigDataXmlHelper;

  @Before
  public void setup() throws ParserConfigurationException, TransformerConfigurationException {
    bigDataXmlHelper = new BigDataXmlHelper();
  }

  @Test
  public void test() throws TransformerException {
    Element testProp1 = bigDataXmlHelper.addTag( "testProp1" );
    bigDataXmlHelper.addTag( testProp1, "key", "value" );
    bigDataXmlHelper.addTag( "test2" );
    System.out.println( bigDataXmlHelper.getString( 4 ) );
  }
}
