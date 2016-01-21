package org.pentaho.bigdata.api.hbase;

import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.io.IOException;

/**
 * Created by bryan on 1/19/16.
 */
public interface ByteConversionUtil {
  int getSizeOfFloat();

  int getSizeOfDouble();

  int getSizeOfInt();

  int getSizeOfLong();

  int getSizeOfShort();

  int getSizeOfByte();

  byte[] toBytes( String var1 );

  byte[] toBytes( int var1 );

  byte[] toBytes( long var1 );

  byte[] toBytes( float var1 );

  byte[] toBytes( double var1 );

  byte[] toBytesBinary( String var1 );

  String toString( byte[] var1 );

  long toLong( byte[] var1 );

  int toInt( byte[] var1 );

  float toFloat( byte[] var1 );

  double toDouble( byte[] var1 );

  short toShort( byte[] var1 );

  byte[] encodeKeyValue( Object keyValue, Mapping.KeyType keyType ) throws KettleException;

  byte[] encodeObject( Object obj ) throws IOException;

  byte[] compoundKey( String... keys ) throws IOException;

  String[] splitKey( byte[] compoundKey ) throws IOException;

  String objectIndexValuesToString( Object[] values );

  Object[] stringIndexListToObjects( String list ) throws IllegalArgumentException;

  byte[] encodeKeyValue( Object o, ValueMetaInterface valueMetaInterface, Mapping.KeyType keyType )
    throws KettleException;

  byte[] encodeColumnValue( Object o, ValueMetaInterface valueMetaInterface, HBaseValueMetaInterface hbaseColMeta )
    throws KettleException;

  boolean isImmutableBytesWritable( Object o );

  Object decodeColumnValue( byte[] bytes, HBaseValueMetaInterface valueMetaInterface ) throws KettleException;

  Object decodeKeyValue( byte[] rowKey, Mapping m_tableMapping ) throws KettleException;
}
