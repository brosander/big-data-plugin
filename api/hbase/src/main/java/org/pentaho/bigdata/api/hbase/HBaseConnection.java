package org.pentaho.bigdata.api.hbase;

import org.pentaho.bigdata.api.hbase.table.HBaseTable;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Created by bryan on 1/19/16.
 */
public interface HBaseConnection extends Closeable {
  public static final String COL_DESCRIPTOR_MAX_VERSIONS_KEY = "col.descriptor.maxVersions";
  public static final String COL_DESCRIPTOR_COMPRESSION_KEY = "col.descriptor.compression";
  public static final String COL_DESCRIPTOR_IN_MEMORY_KEY = "col.descriptor.inMemory";
  public static final String COL_DESCRIPTOR_BLOCK_CACHE_ENABLED_KEY = "col.descriptor.blockCacheEnabled";
  public static final String COL_DESCRIPTOR_BLOCK_SIZE_KEY = "col.descriptor.blockSize";
  public static final String COL_DESCRIPTOR_TIME_TO_LIVE_KEY = "col.desciptor.timeToLive";
  public static final String COL_DESCRIPTOR_BLOOM_FILTER_KEY = "col.descriptor.bloomFilter";
  public static final String COL_DESCRIPTOR_SCOPE_KEY = "col.descriptor.scope";

  HBaseService getService();

  HBaseTable getTable( String tableName ) throws IOException;

  void checkHBaseAvailable() throws IOException;

  List<String> listTableNames() throws IOException;
}
