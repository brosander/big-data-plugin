package org.pentaho.big.data.impl.cluster.tests.hdfs;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.BaseClusterTest;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultEntryImpl;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.impl.cluster.tests.Constants;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemPath;
import org.pentaho.di.i18n.BaseMessages;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Created by bryan on 8/14/15.
 */
public class WriteToAndDeleteFromUsersHomeFolderTest extends BaseClusterTest {
  public static final String HADOOP_FILE_SYSTEM_WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST =
    "hadoopFileSystemWriteToAndDeleteFromUsersHomeFolderTest";
  private static final Class<?> PKG = WriteToAndDeleteFromUsersHomeFolderTest.class;
  private final HadoopFileSystemLocator hadoopFileSystemLocator;

  public WriteToAndDeleteFromUsersHomeFolderTest( HadoopFileSystemLocator hadoopFileSystemLocator ) {
    super( Constants.HADOOP_FILE_SYSTEM, HADOOP_FILE_SYSTEM_WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST,
      BaseMessages.getString( PKG, "WriteToAndDeleteFromUsersHomeFolderTest.Name" ), new HashSet<>(
        Arrays.asList( ListHomeDirectoryTest.HADOOP_FILE_SYSTEM_LIST_HOME_DIRECTORY_TEST ) ) );
    this.hadoopFileSystemLocator = hadoopFileSystemLocator;
  }

  @Override public List<ClusterTestResultEntry> runTest( NamedCluster namedCluster ) {
    List<ClusterTestResultEntry> clusterTestResultEntries = new ArrayList<>();
    try {
      HadoopFileSystem hadoopFilesystem = hadoopFileSystemLocator.getHadoopFilesystem( namedCluster );
      if ( hadoopFilesystem == null ) {
        clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
          BaseMessages.getString( PKG, "WriteToAndDeleteFromUsersHomeFolderTest.CouldntGetFileSystem.Desc" ),
          BaseMessages.getString( PKG, "WriteToAndDeleteFromUsersHomeFolderTest.CouldntGetFileSystem.Message" ) ) );
      } else {
        try {
          HadoopFileSystemPath path = hadoopFilesystem.getPath( "pentaho-shim-test-file.test" );
          HadoopFileSystemPath qualifiedPath = hadoopFilesystem.makeQualified( path );
          if ( hadoopFilesystem.exists( path ) ) {
            clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.WARNING,
              BaseMessages.getString( PKG, "WriteToAndDeleteFromUsersHomeFolderTest.FileExists.Desc" ),
              BaseMessages.getString( PKG, "WriteToAndDeleteFromUsersHomeFolderTest.FileExists.Message",
                qualifiedPath.toString() ) ) );
          } else {
            OutputStream outputStream = hadoopFilesystem.create( path );
            try {
              outputStream.write( "Hello, Cluster".getBytes( Charset.forName( "UTF-8" ) ) );
            } finally {
              try {
                outputStream.close();
              } catch ( IOException e ) {
                //Ignore
              }
            }
            if ( hadoopFilesystem.delete( path, false ) ) {
              clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.INFO,
                BaseMessages.getString( PKG, "WriteToAndDeleteFromUsersHomeFolderTest.Success.Desc" ),
                BaseMessages
                  .getString( PKG, "WriteToAndDeleteFromUsersHomeFolderTest.Success.Message",
                    qualifiedPath.toString() ) ) );
            } else {
              clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.WARNING,
                BaseMessages.getString( PKG, "WriteToAndDeleteFromUsersHomeFolderTest.UnableToDelete.Desc" ),
                BaseMessages
                  .getString( PKG, "WriteToAndDeleteFromUsersHomeFolderTest.UnableToDelete.Message",
                    qualifiedPath.toString() ) ) );
            }
          }
        } catch ( IOException e ) {
          clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
            BaseMessages.getString( PKG, "WriteToAndDeleteFromUsersHomeFolderTest.ErrorListingDirectory.Desc" ),
            BaseMessages.getString( PKG, "WriteToAndDeleteFromUsersHomeFolderTest.ErrorListingDirectory.Message" ),
            e ) );
        }
      }
    } catch ( ClusterInitializationException e ) {
      clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
        BaseMessages.getString( PKG, "WriteToAndDeleteFromUsersHomeFolderTest.ErrorInitializingCluster.Desc" ),
        BaseMessages.getString( PKG, "WriteToAndDeleteFromUsersHomeFolderTest.ErrorInitializingCluster.Message" ),
        e ) );
    }
    return clusterTestResultEntries;
  }
}
