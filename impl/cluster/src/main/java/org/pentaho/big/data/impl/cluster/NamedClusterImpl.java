/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.big.data.impl.cluster;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

import java.util.Comparator;

@MetaStoreElementType( name = "NamedCluster", description = "A NamedCluster" )
public class NamedClusterImpl implements NamedCluster {

  @MetaStoreAttribute
  private String name;
  // Comparator for sorting clusters alphabetically by name
  public static final Comparator<NamedCluster> comparator = new Comparator<NamedCluster>() {
    @Override
    public int compare( NamedCluster c1, NamedCluster c2 ) {
      return c1.getName().compareToIgnoreCase( c2.getName() );
    }
  };
  @MetaStoreAttribute
  private String hdfsHost;
  @MetaStoreAttribute
  private String hdfsPort;
  @MetaStoreAttribute
  private String hdfsUsername;
  @MetaStoreAttribute( password = true )
  private String hdfsPassword;
  @MetaStoreAttribute
  private String jobTrackerHost;
  @MetaStoreAttribute
  private String jobTrackerPort;
  @MetaStoreAttribute
  private String zooKeeperHost;
  @MetaStoreAttribute
  private String zooKeeperPort;
  @MetaStoreAttribute
  private String oozieUrl;
  @MetaStoreAttribute
  private boolean mapr;
  @MetaStoreAttribute
  private long lastModifiedDate = System.currentTimeMillis();

  @Override public String getName() {
    return name;
  }

  @Override public void setName( String name ) {
    this.name = name;
  }

  @Override public void replaceMeta( NamedCluster nc ) {
    this.setName( nc.getName() );
    this.setHdfsHost( nc.getHdfsHost() );
    this.setHdfsPort( nc.getHdfsPort() );
    this.setHdfsUsername( nc.getHdfsUsername() );
    this.setHdfsPassword( nc.getHdfsPassword() );
    this.setJobTrackerHost( nc.getJobTrackerHost() );
    this.setJobTrackerPort( nc.getJobTrackerPort() );
    this.setZooKeeperHost( nc.getZooKeeperHost() );
    this.setZooKeeperPort( nc.getZooKeeperPort() );
    this.setOozieUrl( nc.getOozieUrl() );
    this.setMapr( nc.isMapr() );
    this.lastModifiedDate = System.currentTimeMillis();
  }

  public NamedCluster clone() {
    NamedCluster nc = new NamedClusterImpl();
    nc.replaceMeta( this );
    return nc;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals( Object obj ) {
    if ( this == obj ) {
      return true;
    }
    if ( obj == null ) {
      return false;
    }
    if ( getClass() != obj.getClass() ) {
      return false;
    }
    NamedClusterImpl other = (NamedClusterImpl) obj;
    if ( name == null ) {
      if ( other.name != null ) {
        return false;
      }
    } else if ( !name.equals( other.name ) ) {
      return false;
    }
    return true;
  }

  @Override public String getHdfsHost() {
    return hdfsHost;
  }

  @Override public void setHdfsHost( String hdfsHost ) {
    this.hdfsHost = hdfsHost;
  }

  @Override public String getHdfsPort() {
    return hdfsPort;
  }

  @Override public void setHdfsPort( String hdfsPort ) {
    this.hdfsPort = hdfsPort;
  }

  @Override public String getHdfsUsername() {
    return hdfsUsername;
  }

  @Override public void setHdfsUsername( String hdfsUsername ) {
    this.hdfsUsername = hdfsUsername;
  }

  @Override public String getHdfsPassword() {
    return hdfsPassword;
  }

  @Override public void setHdfsPassword( String hdfsPassword ) {
    this.hdfsPassword = hdfsPassword;
  }

  @Override public String getJobTrackerHost() {
    return jobTrackerHost;
  }

  @Override public void setJobTrackerHost( String jobTrackerHost ) {
    this.jobTrackerHost = jobTrackerHost;
  }

  @Override public String getJobTrackerPort() {
    return jobTrackerPort;
  }

  @Override public void setJobTrackerPort( String jobTrackerPort ) {
    this.jobTrackerPort = jobTrackerPort;
  }

  @Override public String getZooKeeperHost() {
    return zooKeeperHost;
  }

  @Override public void setZooKeeperHost( String zooKeeperHost ) {
    this.zooKeeperHost = zooKeeperHost;
  }

  @Override public String getZooKeeperPort() {
    return zooKeeperPort;
  }

  @Override public void setZooKeeperPort( String zooKeeperPort ) {
    this.zooKeeperPort = zooKeeperPort;
  }

  @Override public String getOozieUrl() {
    return oozieUrl;
  }

  @Override public void setOozieUrl( String oozieUrl ) {
    this.oozieUrl = oozieUrl;
  }

  @Override public long getLastModifiedDate() {
    return lastModifiedDate;
  }

  @Override public void setLastModifiedDate( long lastModifiedDate ) {
    this.lastModifiedDate = lastModifiedDate;
  }

  @Override public boolean isMapr() {
    return mapr;
  }

  @Override public void setMapr( boolean mapr ) {
    this.mapr = mapr;
  }

}
