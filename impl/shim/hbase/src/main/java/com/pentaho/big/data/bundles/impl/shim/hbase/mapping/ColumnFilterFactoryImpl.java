package com.pentaho.big.data.bundles.impl.shim.hbase.mapping;

import org.pentaho.bigdata.api.hbase.mapping.ColumnFilter;
import org.pentaho.bigdata.api.hbase.mapping.ColumnFilterFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;

/**
 * Created by bryan on 1/21/16.
 */
public class ColumnFilterFactoryImpl implements ColumnFilterFactory {
  @Override public ColumnFilter createFilter( Node filterNode ) {
    return new ColumnFilterImpl( org.pentaho.hbase.shim.api.ColumnFilter.getFilter( filterNode ) );
  }

  @Override public ColumnFilter createFilter( Repository rep, int nodeNum, ObjectId id_step ) throws KettleException {
    return new ColumnFilterImpl( org.pentaho.hbase.shim.api.ColumnFilter.getFilter( rep, nodeNum, id_step ) );
  }

  @Override public ColumnFilter createFilter( String alias ) {
    return new ColumnFilterImpl( new org.pentaho.hbase.shim.api.ColumnFilter( alias ) );
  }
}
