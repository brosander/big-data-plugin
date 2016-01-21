package org.pentaho.bigdata.api.hbase.mapping;

/**
 * Created by bryan on 1/21/16.
 */
public interface MappingFactory {
  Mapping createMapping();

  Mapping createMapping( String tableName, String mappingName );

  Mapping createMapping( String tableName, String mappingName, String keyName, Mapping.KeyType keyType );
}
