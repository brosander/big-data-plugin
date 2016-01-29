package org.pentaho.bigdata.api.hbase;

/**
 * Created by bryan on 1/29/16.
 */
public interface ResultFactory {
  boolean canHandle( Object object );

  Result create( Object object ) throws ResultFactoryException;
}
