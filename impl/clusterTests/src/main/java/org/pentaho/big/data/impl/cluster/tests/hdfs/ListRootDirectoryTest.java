package org.pentaho.big.data.impl.cluster.tests.hdfs;

import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.di.i18n.BaseMessages;

/**
 * Created by bryan on 8/14/15.
 */
public class ListRootDirectoryTest extends ListDirectoryTest {
  public static final String HADOOP_FILE_SYSTEM_LIST_ROOT_DIRECTORY_TEST =
    "hadoopFileSystemListRootDirectoryTest";
  private static final Class<?> PKG = ListRootDirectoryTest.class;

  public ListRootDirectoryTest( MessageGetterFactory messageGetterFactory,
                                HadoopFileSystemLocator hadoopFileSystemLocator ) {
    super( messageGetterFactory, hadoopFileSystemLocator, "/", HADOOP_FILE_SYSTEM_LIST_ROOT_DIRECTORY_TEST,
      BaseMessages.getString( PKG, "ListRootDirectoryTest.Name" ) );
  }
}
