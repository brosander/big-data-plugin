package org.pentaho.bigdata.kettle.plugins.common;

import org.pentaho.bigdata.api.configuration.ConfigurationNamespace;
import org.pentaho.bigdata.api.configuration.NamedConfiguration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.util.Iterator;

/**
 * Created by bryan on 6/19/15.
 */
public class BigDataXmlHelper {
  private final Document document;
  private final Element rootElement;

  public BigDataXmlHelper() throws ParserConfigurationException, TransformerConfigurationException {
    document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    rootElement = document.createElement( "root_element" );
    document.appendChild( rootElement );
  }

  private Element parentElement( Element parent ) {
    if ( parent == null ) {
      return rootElement;
    }
    return parent;
  }

  public Element writeNamedConfiguration( NamedConfiguration namedConfiguration ) {
    return writeNamedConfiguration( null, namedConfiguration );
  }

  public Element writeNamedConfiguration( Element parent, NamedConfiguration namedConfiguration ) {
    Element namedConfigurationElement = addTag( parent, "namedConfiguration" );
    String name = namedConfiguration.getName();
    if ( name != null ) {
      addTag( namedConfigurationElement, "name", name );
    }
    writeConfigurationNamespace( namedConfigurationElement, namedConfiguration );
    for ( String namespace : namedConfiguration.getConfigurationNamespaces() ) {
      writeConfigurationNamespace( addTag( namedConfigurationElement, "configurationNamespace" ),
        namedConfiguration.getConfigurationNamespace( namespace ) );
    }
    return namedConfigurationElement;
  }

  private Element writeConfigurationNamespace( Element parent, ConfigurationNamespace configurationNamespace ) {
    Element properties = addTag( parent, "properties" );
    for ( String propertyName : configurationNamespace.getProperties() ) {
      String propertyValue = configurationNamespace.getProperty( propertyName );
      if ( propertyValue != null ) {
        Element property = addTag( properties, "property" );
        addTag( property, "key", propertyName );
        addTag( property, "value", propertyValue );
      }
    }
    return properties;
  }

  public Element addTag( String tagName ) {
    return addTag( tagName, null );
  }

  public Element addTag( String tagName, String value ) {
    return addTag( null, tagName, value );
  }

  public Element addTag( Element parent, String tagName ) {
    return addTag( parent, tagName, null );
  }

  public Element addTag( Element parent, String tagName, String value ) {
    Element element = document.createElement( tagName );
    if ( value != null ) {
      element.setTextContent( value );
    }
    parentElement( parent ).appendChild( element );
    return element;
  }

  public String boolToString( Boolean value ) {
    if ( value == null ) {
      return null;
    }
    return value ? "Y" : "N";
  }

  public Iterable<Node> iterable( final NodeList nodeList ) {
    return new Iterable<Node>() {
      @Override public Iterator<Node> iterator() {
        return new Iterator<Node>() {
          int index = 0;

          @Override public boolean hasNext() {
            return index < nodeList.getLength();
          }

          @Override public Node next() {
            return nodeList.item( index++ );
          }

          @Override public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  public String getString( int indentLevel ) throws TransformerException {
    Transformer transformer = TransformerFactory.newInstance().newTransformer();
    transformer.setOutputProperty( OutputKeys.OMIT_XML_DECLARATION, "yes" );
    transformer.setOutputProperty( OutputKeys.INDENT, "yes" );
    transformer.setOutputProperty( "{http://xml.apache.org/xslt}indent-amount", "2" );
    StringBuilder indentSb = new StringBuilder();
    for ( int i = 0; i < indentLevel; i++ ) {
      indentSb.append( " " );
    }
    String indent = indentSb.toString();
    StringWriter stringWriter = new StringWriter();
    StreamResult streamResult = new StreamResult( stringWriter );
    for ( Node node : iterable( rootElement.getChildNodes() ) ) {
      DOMSource domSource = new DOMSource( node );
      transformer.transform( domSource, streamResult );
    }
    String xmlTrimmed = stringWriter.toString().trim().replace( System.lineSeparator(),
      System.lineSeparator() + indent );
    StringBuilder result =
      new StringBuilder( indent.length() + xmlTrimmed.length() + System.lineSeparator().length() ).append( indent )
        .append( xmlTrimmed ).append( System.lineSeparator() );
    return result.toString();
  }
}
