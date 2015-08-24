package org.pentaho.big.data.impl.cluster.tests.hdfs;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.clusterTest.TestMessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetter;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 8/24/15.
 */
public class ListHomeDirectoryTestTest {
  private MessageGetterFactory messageGetterFactory;
  private HadoopFileSystemLocator hadoopFileSystemLocator;
  private ListHomeDirectoryTest listHomeDirectoryTest;
  private MessageGetter messageGetter;

  @Before
  public void setup() {
    messageGetterFactory = new TestMessageGetterFactory();
    messageGetter = messageGetterFactory.create( ListHomeDirectoryTest.class );
    hadoopFileSystemLocator = mock( HadoopFileSystemLocator.class );
    listHomeDirectoryTest = new ListHomeDirectoryTest( messageGetterFactory, hadoopFileSystemLocator );
  }

  @Test
  public void testGetName() {
    assertEquals( messageGetter.getMessage( ListHomeDirectoryTest.LIST_HOME_DIRECTORY_TEST_NAME ),
      listHomeDirectoryTest.getName() );
  }
}
