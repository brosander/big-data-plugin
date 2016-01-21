package com.pentaho.big.data.bundles.impl.shim.hbase.mapping;

import org.pentaho.bigdata.api.hbase.ByteConversionUtil;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.hbase.shim.api.HBaseValueMeta;
import org.w3c.dom.Node;

import java.util.Map;

/**
 * Created by bryan on 1/21/16.
 */
public class MappingImpl implements Mapping {
  private final org.pentaho.hbase.shim.api.Mapping delegate;

  public MappingImpl( org.pentaho.hbase.shim.api.Mapping delegate, ByteConversionUtil byteConversionUtil ) {
    this.delegate = delegate;
  }

  @Override public String addMappedColumn( HBaseValueMetaInterface column, boolean isTupleColumn ) throws Exception {
    return delegate.addMappedColumn( (HBaseValueMeta) column, isTupleColumn );
  }

  @Override public String getTableName() {
    return delegate.getTableName();
  }

  @Override public void setTableName( String tableName ) {
    delegate.setTableName( tableName );
  }

  @Override public String getMappingName() {
    return delegate.getMappingName();
  }

  @Override public void setMappingName( String mappingName ) {
    delegate.setMappingName( mappingName );
  }

  @Override public String getKeyName() {
    return delegate.getKeyName();
  }

  @Override public void setKeyName( String keyName ) {
    delegate.setKeyName( keyName );
  }

  @Override public void setKeyTypeAsString( String type ) throws Exception {
    delegate.setKeyTypeAsString( type );
  }

  @Override public KeyType getKeyType() {
    org.pentaho.hbase.shim.api.Mapping.KeyType keyType = delegate.getKeyType();
    if ( keyType == null ) {
      return null;
    }
    return KeyType.valueOf( keyType.name() );
  }

  @Override public void setKeyType( KeyType type ) {
    if ( type == null ) {
      delegate.setKeyType( null );
    } else {
      delegate.setKeyType( org.pentaho.hbase.shim.api.Mapping.KeyType.valueOf( type.name() ) );
    }
  }

  @Override public boolean isTupleMapping() {
    return delegate.isTupleMapping();
  }

  @Override public void setTupleMapping( boolean t ) {
    delegate.setTupleMapping( t );
  }

  @Override public String getTupleFamilies() {
    return delegate.getTupleFamilies();
  }

  @Override public void setTupleFamilies( String f ) {
    delegate.setTupleFamilies( f );
  }

  @Override public String[] getTupleFamiliesSplit() {
    return getTupleFamilies().split( HBaseValueMeta.SEPARATOR );
  }

  @Override public Map<String, HBaseValueMetaInterface> getMappedColumns() {
    return delegate.getMappedColumns();
  }

  @Override public void setMappedColumns( Map<String, HBaseValueMetaInterface> cols ) {
    delegate.setMappedColumns( cols );
  }

  @Override public void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    delegate.saveRep( rep, id_transformation, id_step );
  }

  @Override public String getXML() {
    return delegate.getXML();
  }

  @Override public boolean loadXML( Node stepnode ) throws KettleXMLException {
    return delegate.loadXML( stepnode );
  }

  @Override public boolean readRep( Repository rep, ObjectId id_step ) throws KettleException {
    return delegate.readRep( rep, id_step );
  }

  @Override public String getFriendlyName() {
    return delegate.getMappingName() + HBaseValueMeta.SEPARATOR + delegate.getTableName();
  }
}
