package org.pentaho.big.data.impl.cluster.tests.hdfs;

import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.di.i18n.BaseMessages;

/**
 * Created by bryan on 8/14/15.
 */
public class ListHomeDirectoryTest extends ListDirectoryTest {
  public static final String HADOOP_FILE_SYSTEM_LIST_HOME_DIRECTORY_TEST =
    "hadoopFileSystemListHomeDirectoryTest";
  private static final Class<?> PKG = ListHomeDirectoryTest.class;

  public ListHomeDirectoryTest( MessageGetterFactory messageGetterFactory,
                                HadoopFileSystemLocator hadoopFileSystemLocator ) {
    super( messageGetterFactory, hadoopFileSystemLocator, "", HADOOP_FILE_SYSTEM_LIST_HOME_DIRECTORY_TEST,
      BaseMessages.getString( PKG, "ListHomeDirectoryTest.Name" ) );
  }
}
