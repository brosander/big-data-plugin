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

import com.pentaho.big.data.bundles.impl.configuration.ConfigurationNamespaceImpl;
import com.pentaho.big.data.bundles.impl.configuration.NamedConfigurationImpl;
import org.apache.commons.vfs.Capability;
import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystem;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.UserAuthenticationData;
import org.apache.commons.vfs.impl.DefaultFileSystemManager;
import org.apache.commons.vfs.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs.provider.GenericFileName;
import org.pentaho.bigdata.api.configuration.ConfigurationNamespace;
import org.pentaho.bigdata.api.configuration.NamedConfiguration;
import org.pentaho.bigdata.api.configuration.NamedConfigurationLocator;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.di.core.vfs.KettleVFS;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
  private final NamedConfigurationLocator namedConfigurationLocator;

  public HDFSFileProvider( HadoopFileSystemLocator hadoopFileSystemLocator,
                           NamedConfigurationLocator namedConfigurationLocator ) throws FileSystemException {
    super();
    this.hadoopFileSystemLocator = hadoopFileSystemLocator;
    this.namedConfigurationLocator = namedConfigurationLocator;
    setFileNameParser( HDFSFileNameParser.getInstance() );
    ( (DefaultFileSystemManager) KettleVFS.getInstance().getFileSystemManager() ).addProvider( "hdfs", this );
  }

  protected FileSystem doCreateFileSystem( final FileName name, final FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    String rootURI = name.getRootURI();
    NamedConfiguration namedConfiguration = null;
    String hostName = ( (GenericFileName) name.getRoot() ).getHostName();
    int port = ( (GenericFileName) name.getRoot() ).getPort();
    if ( rootURI.contains( "@" ) || rootURI.substring( name.getScheme().length() + 1 ).contains( ":" ) ) {
      // More than hostname is specified,  Must not be a named config
    } else {
      namedConfiguration = namedConfigurationLocator.get( hostName );
    }
    if ( namedConfiguration == null ) {
      Map<String, ConfigurationNamespace> configurationNamespaceMap = new HashMap<String, ConfigurationNamespace>();
      Map<String, String> properties = new HashMap<String, String>();
      String fsDefault = name.getScheme() + "://" + hostName;
      if ( port > 0 ) {
        fsDefault = fsDefault + ":" + port;
      }
      properties.put( HadoopFileSystem.FS_DEFAULT_NAME, fsDefault );
      configurationNamespaceMap.put( SCHEME, new ConfigurationNamespaceImpl( properties ) );
      namedConfiguration = new NamedConfigurationImpl( new HashMap<String, String>(), configurationNamespaceMap );
    }
    return new HDFSFileSystem( name, fileSystemOptions,
      hadoopFileSystemLocator.getHadoopFilesystem( namedConfiguration ) );
  }

  public Collection<Capability> getCapabilities() {
    return capabilities;
  }
}
