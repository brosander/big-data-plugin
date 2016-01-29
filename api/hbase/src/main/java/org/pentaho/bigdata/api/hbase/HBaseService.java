package org.pentaho.bigdata.api.hbase;

import org.pentaho.bigdata.api.hbase.mapping.ColumnFilterFactory;
import org.pentaho.bigdata.api.hbase.mapping.MappingFactory;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterfaceFactory;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;

import java.io.IOException;
import java.util.List;

/**
 * Created by bryan on 1/19/16.
 */
public interface HBaseService {
  HBaseConnection getHBaseConnection( VariableSpace variableSpace, String siteConfig, String defaultConfig, LogChannelInterface logChannelInterface )
    throws IOException;

  ColumnFilterFactory getColumnFilterFactory();

  MappingFactory getMappingFactory();

  HBaseValueMetaInterfaceFactory getHBaseValueMetaInterfaceFactory();

  ByteConversionUtil getByteConversionUtil();

  ResultFactory getResultFactory();
}
