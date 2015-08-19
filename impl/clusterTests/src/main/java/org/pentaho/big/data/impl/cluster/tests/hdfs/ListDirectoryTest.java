package org.pentaho.big.data.impl.cluster.tests.hdfs;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResult;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.BaseClusterTest;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultEntryImpl;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultImpl;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.impl.cluster.tests.Constants;
import org.pentaho.bigdata.api.hdfs.HadoopFileStatus;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemPath;
import org.pentaho.bigdata.api.hdfs.exceptions.AccessControlException;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Created by bryan on 8/14/15.
 */
public class ListDirectoryTest extends BaseClusterTest {
  private static final Class<?> PKG = ListDirectoryTest.class;
  private final HadoopFileSystemLocator hadoopFileSystemLocator;
  private final String directory;

  public ListDirectoryTest( HadoopFileSystemLocator hadoopFileSystemLocator, String directory, String id,
                            String name ) {
    super( Constants.HADOOP_FILE_SYSTEM, id, name, new HashSet<>(
      Arrays.asList( PingFileSystemEntryPointTest.HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST ) ) );
    this.hadoopFileSystemLocator = hadoopFileSystemLocator;
    this.directory = directory;
  }

  @Override public ClusterTestResult runTest( NamedCluster namedCluster ) {
    List<ClusterTestResultEntry> clusterTestResultEntries = new ArrayList<>();
    try {
      HadoopFileSystem hadoopFilesystem = hadoopFileSystemLocator.getHadoopFilesystem( namedCluster );
      if ( hadoopFilesystem == null ) {
        clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
          BaseMessages.getString( PKG, "ListDirectoryTest.CouldntGetFileSystem.Desc" ),
          BaseMessages.getString( PKG, "ListDirectoryTest.CouldntGetFileSystem.Message", namedCluster.getName() ) ) );
      } else {
        HadoopFileSystemPath hadoopFilesystemPath;
        if ( Const.isEmpty( directory ) ) {
          hadoopFilesystemPath = hadoopFilesystem.getHomeDirectory();
        } else {
          hadoopFilesystemPath = hadoopFilesystem.getPath( directory );
        }
        try {
          HadoopFileStatus[] hadoopFileStatuses = hadoopFilesystem.listStatus( hadoopFilesystemPath );
          StringBuilder paths = new StringBuilder();
          for ( HadoopFileStatus hadoopFileStatus : hadoopFileStatuses ) {
            paths.append( hadoopFileStatus.getPath() );
            paths.append( ", " );
          }
          if ( paths.length() > 0 ) {
            paths.setLength( paths.length() - 2 );
          }
          clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.INFO,
            BaseMessages.getString( PKG, "ListDirectoryTest.Success.Desc" ),
            BaseMessages.getString( PKG, "ListDirectoryTest.Success.Message", paths.toString() ) ) );
        } catch ( AccessControlException e ) {
          clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.WARNING,
            BaseMessages.getString( PKG, "ListDirectoryTest.AccessControlException.Desc" ),
            BaseMessages
              .getString( PKG, "ListDirectoryTest.AccessControlException.Message", hadoopFilesystemPath.toString() ),
            e ) );
        } catch ( IOException e ) {
          clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
            BaseMessages.getString( PKG, "ListDirectoryTest.ErrorListingDirectory.Desc" ),
            BaseMessages
              .getString( PKG, "ListDirectoryTest.ErrorListingDirectory.Message", hadoopFilesystemPath.toString() ),
            e ) );
        }
      }
    } catch ( ClusterInitializationException e ) {
      clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
        BaseMessages.getString( PKG, "ListDirectoryTest.ErrorInitializingCluster.Desc" ),
        BaseMessages.getString( PKG, "ListDirectoryTest.ErrorInitializingCluster.Message", namedCluster.getName() ),
        e ) );
    }
    return new ClusterTestResultImpl( this, clusterTestResultEntries );
  }
}
