package org.pentaho.bigdata.api.hbase.mapping;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;

/**
 * Created by bryan on 1/19/16.
 */
public interface ColumnFilterFactory {
  ColumnFilter createFilter( Node filterNode );

  ColumnFilter createFilter( Repository rep, int nodeNum, ObjectId id_step ) throws KettleException;

  ColumnFilter createFilter(String alias);
}
