package org.pentaho.bigdata.api.hbase.meta;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;

/**
 * Created by bryan on 1/19/16.
 */
public interface HBaseValueMetaInterface extends ValueMetaInterface {
  boolean isKey();

  void setKey( boolean key );

  String getAlias();

  void setAlias( String alias );

  String getColumnName();

  void setColumnName( String columnName );

  String getColumnFamily();

  void setColumnFamily( String family );

  void setHBaseTypeFromString( String hbaseType ) throws IllegalArgumentException;

  String getHBaseTypeDesc();

  Object decodeColumnValue( byte[] rawColValue ) throws KettleException;

  String getTableName();

  void setTableName( String tableName );

  String getMappingName();

  void setMappingName( String mappingName );

  boolean getIsLongOrDouble();

  void setIsLongOrDouble( boolean ld );

  Object[] stringIndexListToObjects(String list) throws IllegalArgumentException;
}
