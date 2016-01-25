package org.pentaho.bigdata.api.hbase.meta;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;

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

  byte[] encodeColumnValue( Object o, ValueMetaInterface valueMetaInterface ) throws KettleException;

  void getXml( StringBuilder stringBuilder );

  void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step, int count ) throws KettleException;
}
