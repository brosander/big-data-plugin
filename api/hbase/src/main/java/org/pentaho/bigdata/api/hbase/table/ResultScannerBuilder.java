package org.pentaho.bigdata.api.hbase.table;

import org.pentaho.bigdata.api.hbase.mapping.ColumnFilter;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;

import java.io.IOException;

/**
 * Created by bryan on 1/19/16.
 */
public interface ResultScannerBuilder {
  void addColumnToScan( String colFamilyName, String colName, boolean colNameIsBinary ) throws IOException;

  void addColumnFilterToScan( ColumnFilter cf, HBaseValueMetaInterface columnMeta, VariableSpace vars,
                              boolean matchAny )
    throws IOException;

  void setCaching( int cacheSize );

  ResultScanner build() throws IOException;
}
