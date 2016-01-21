package com.pentaho.big.data.bundles.impl.shim.hbase.mapping;

import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.bigdata.api.hbase.mapping.MappingFactory;

/**
 * Created by bryan on 1/21/16.
 */
public class MappingFactoryImpl implements MappingFactory {
  @Override public Mapping createMapping() {
    return null;
  }

  @Override public Mapping createMapping( String tableName, String mappingName ) {
    return null;
  }

  @Override
  public Mapping createMapping( String tableName, String mappingName, String keyName, Mapping.KeyType keyType ) {
    return null;
  }
}
