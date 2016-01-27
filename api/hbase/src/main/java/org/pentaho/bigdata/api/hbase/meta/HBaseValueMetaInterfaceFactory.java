package org.pentaho.bigdata.api.hbase.meta;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Created by bryan on 1/21/16.
 */
public interface HBaseValueMetaInterfaceFactory {
  HBaseValueMetaInterface createHBaseValueMetaInterface( String family, String column, String alias, int type,
                                                         int length, int precision )
    throws IllegalArgumentException;

  HBaseValueMetaInterface createHBaseValueMetaInterface( String name, int type, int length, int precision )
    throws IllegalArgumentException;

  List<HBaseValueMetaInterface> createListFromRepository( Repository rep, ObjectId id_step ) throws KettleException;

  HBaseValueMetaInterface createFromRepository( Repository rep, ObjectId id_step, int count ) throws KettleException;

  List<HBaseValueMetaInterface> createListFromNode( Node stepNode ) throws KettleXMLException;

  HBaseValueMetaInterface createFromNode( Node metaNode ) throws KettleXMLException;

  HBaseValueMetaInterface copy( HBaseValueMetaInterface hBaseValueMetaInterface );
}