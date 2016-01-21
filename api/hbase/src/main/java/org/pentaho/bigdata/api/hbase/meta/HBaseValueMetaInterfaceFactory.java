package org.pentaho.bigdata.api.hbase.meta;

/**
 * Created by bryan on 1/21/16.
 */
public interface HBaseValueMetaInterfaceFactory {
  HBaseValueMetaInterface createHBaseValueMetaInterface( String family, String column, String alias, int type,
                                                         int length, int precision )
    throws IllegalArgumentException;

  HBaseValueMetaInterface createHBaseValueMetaInterface( String name, int type, int length, int precision )
    throws IllegalArgumentException;
}
