/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package com.pentaho.big.data.bundles.impl.vfs.hdfs;

import org.apache.commons.vfs.Capability;
import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystem;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.UserAuthenticationData;
import org.apache.commons.vfs.impl.DefaultFileSystemManager;
import org.apache.commons.vfs.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs.provider.GenericFileName;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class HDFSFileProvider extends AbstractOriginatingFileProvider {
  /**
   * The scheme this provider was designed to support
   */
  public static final String SCHEME = "hdfs";
  /**
   * User Information.
   */
  public static final String ATTR_USER_INFO = "UI";
  /**
   * Authentication types.
   */
  public static final UserAuthenticationData.Type[] AUTHENTICATOR_TYPES =
    new UserAuthenticationData.Type[] { UserAuthenticationData.USERNAME,
      UserAuthenticationData.PASSWORD };
  /**
   * The provider's capabilities.
   */
  protected static final Collection<Capability> capabilities =
    Collections.unmodifiableCollection( Arrays.asList( new Capability[] { Capability.CREATE, Capability.DELETE,
      Capability.RENAME, Capability.GET_TYPE, Capability.LIST_CHILDREN, Capability.READ_CONTENT, Capability.URI,
      Capability.WRITE_CONTENT, Capability.APPEND_CONTENT,
      Capability.GET_LAST_MODIFIED, Capability.SET_LAST_MODIFIED_FILE, Capability.RANDOM_ACCESS_READ } ) );
  private final HadoopFileSystemLocator hadoopFileSystemLocator;
  private final NamedClusterService namedClusterService;

  public HDFSFileProvider( HadoopFileSystemLocator hadoopFileSystemLocator,
                           NamedClusterService namedClusterService ) throws FileSystemException {
    super();
    this.hadoopFileSystemLocator = hadoopFileSystemLocator;
    this.namedClusterService = namedClusterService;
    setFileNameParser( HDFSFileNameParser.getInstance() );
    ( (DefaultFileSystemManager) KettleVFS.getInstance().getFileSystemManager() ).addProvider( "hdfs", this );
  }

  protected FileSystem doCreateFileSystem( final FileName name, final FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    String rootURI = name.getRootURI();
    NamedCluster namedCluster = null;
    // TODO
    IMetaStore metaStore = null;
    String hostName = ( (GenericFileName) name.getRoot() ).getHostName();
    int port = ( (GenericFileName) name.getRoot() ).getPort();
    if ( metaStore == null || rootURI.contains( "@" ) || rootURI.substring( name.getScheme().length() + 1 )
      .contains( ":" ) ) {
      // More than hostname is specified,  Must not be a named config
    } else {
      try {
        namedCluster = namedClusterService.read( hostName, metaStore );
      } catch ( MetaStoreException e ) {
        throw new FileSystemException( e );
      }
    }
    if ( namedCluster == null ) {
      namedCluster = namedClusterService.getClusterTemplate();
      namedCluster.setHdfsHost( hostName );
      if ( port > 0 ) {
        namedCluster.setHdfsPort( String.valueOf( port ) );
      }
    }
    return new HDFSFileSystem( name, fileSystemOptions, hadoopFileSystemLocator.getHadoopFilesystem( namedCluster ) );
  }

  public Collection<Capability> getCapabilities() {
    return capabilities;
  }
}
