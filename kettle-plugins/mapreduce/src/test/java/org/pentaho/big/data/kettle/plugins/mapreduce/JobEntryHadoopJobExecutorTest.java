/*******************************************************************************
 *
 * Pentaho Big Data
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

package org.pentaho.big.data.kettle.plugins.mapreduce;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.mapreduce.entry.JobEntryHadoopJobExecutor;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * User: Dzmitry Stsiapanau Date: 9/11/14 Time: 1:36 PM
 */
public class JobEntryHadoopJobExecutorTest {
  private NamedClusterService namedClusterService;
  private RuntimeTestActionService runtimeTestActionService;
  private RuntimeTester runtimeTester;
  private NamedClusterServiceLocator namedClusterServiceLocator;
  private JobEntryHadoopJobExecutor jobExecutor;
  private IMetaStore metaStore;
  private NamedCluster namedCluster;
  private Repository repository;
  private ObjectId objectId;

  @Before
  public void setup() {
    namedClusterService = mock( NamedClusterService.class );
    runtimeTestActionService = mock( RuntimeTestActionService.class );
    runtimeTester = mock( RuntimeTester.class );
    namedClusterServiceLocator = mock( NamedClusterServiceLocator.class );
    metaStore = mock( IMetaStore.class );
    namedCluster = mock( NamedCluster.class );
    repository = mock( Repository.class );
    when( repository.getMetaStore() ).thenReturn( metaStore );
    objectId = mock( ObjectId.class );

    jobExecutor = new JobEntryHadoopJobExecutor( namedClusterService, runtimeTestActionService, runtimeTester,
      namedClusterServiceLocator );
  }

  @Test
  public void testResolveJobUrl() throws MalformedURLException {
    String variableValue = "http://jar.net/url";
    String testvar = "testvar";
    jobExecutor.setVariable( testvar, variableValue );
    assertEquals( new URL( variableValue ), jobExecutor.resolveJarUrl( "${" + testvar + "}" ) );
  }

  private void addTag( StringBuilder builder, String tag, String value ) {
    builder.append( "<" ).append( tag ).append( ">" );
    builder.append( value );
    builder.append( "</" ).append( tag ).append( ">" );
  }

  @Test
  public void testLoadClusterConfigFoundXml()
    throws ParserConfigurationException, IOException, SAXException, MetaStoreException {
    String testName = "testName";
    when( namedClusterService.contains( testName, metaStore ) ).thenReturn( true );
    when( namedClusterService.read( testName, metaStore ) ).thenReturn( namedCluster );
    StringBuilder stringBuilder = new StringBuilder( "<job>" );
    addTag( stringBuilder, JobEntryHadoopJobExecutor.CLUSTER_NAME, testName );
    stringBuilder.append( "</job>" );
    String xml = stringBuilder.toString();
    Element documentElement =
      DocumentBuilderFactory.newInstance().newDocumentBuilder().parse( new ByteArrayInputStream( xml.getBytes() ) )
        .getDocumentElement();
    jobExecutor.loadClusterConfig( null, null, metaStore, documentElement );
    assertEquals( namedCluster, jobExecutor.getNamedCluster() );
  }

  @Test
  public void testLoadClusterConfigNotFoundXml()
    throws ParserConfigurationException, IOException, SAXException, MetaStoreException {
    String testName = "testName";
    String testHost = "testHost";
    String hdfsPort = "8080";
    String jobTrackerHost = "jobTrackerHost";
    String jobTrackerPort = "8081";
    when( namedClusterService.contains( testName, metaStore ) ).thenReturn( false );
    when( namedClusterService.getClusterTemplate() ).thenReturn( namedCluster );
    StringBuilder stringBuilder = new StringBuilder( "<job>" );
    addTag( stringBuilder, JobEntryHadoopJobExecutor.CLUSTER_NAME, testName );
    addTag( stringBuilder, JobEntryHadoopJobExecutor.HDFS_HOSTNAME, testHost );
    addTag( stringBuilder, JobEntryHadoopJobExecutor.HDFS_PORT, hdfsPort );
    addTag( stringBuilder, JobEntryHadoopJobExecutor.JOB_TRACKER_HOSTNAME, jobTrackerHost );
    addTag( stringBuilder, JobEntryHadoopJobExecutor.JOB_TRACKER_PORT, jobTrackerPort );
    stringBuilder.append( "</job>" );
    String xml = stringBuilder.toString();
    Element documentElement =
      DocumentBuilderFactory.newInstance().newDocumentBuilder().parse( new ByteArrayInputStream( xml.getBytes() ) )
        .getDocumentElement();
    jobExecutor.loadClusterConfig( null, null, metaStore, documentElement );
    assertEquals( namedCluster, jobExecutor.getNamedCluster() );
    verify( namedCluster ).setHdfsHost( testHost );
    verify( namedCluster ).setHdfsPort( hdfsPort );
    verify( namedCluster ).setJobTrackerHost( jobTrackerHost );
    verify( namedCluster ).setJobTrackerPort( jobTrackerPort );
  }

  @Test
  public void testLoadClusterConfigFoundRepo()
    throws ParserConfigurationException, IOException, SAXException, MetaStoreException, KettleException {
    String testName = "testName";
    when( namedClusterService.contains( testName, metaStore ) ).thenReturn( true );
    when( namedClusterService.read( testName, metaStore ) ).thenReturn( namedCluster );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.CLUSTER_NAME ) ).thenReturn( testName );
    jobExecutor.loadClusterConfig( objectId, repository, metaStore, null );
    assertEquals( namedCluster, jobExecutor.getNamedCluster() );
  }

  @Test
  public void testLoadClusterConfigNotFoundRepo()
    throws ParserConfigurationException, IOException, SAXException, MetaStoreException, KettleException {
    String testName = "testName";
    String testHost = "testHost";
    String hdfsPort = "8080";
    String jobTrackerHost = "jobTrackerHost";
    String jobTrackerPort = "8081";
    when( namedClusterService.contains( testName, metaStore ) ).thenReturn( false );
    when( namedClusterService.getClusterTemplate() ).thenReturn( namedCluster );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.CLUSTER_NAME ) ).thenReturn( testName );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.HDFS_HOSTNAME ) ).thenReturn( testHost );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.HDFS_PORT ) ).thenReturn( hdfsPort );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.JOB_TRACKER_HOSTNAME ) ).thenReturn( jobTrackerHost );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.JOB_TRACKER_PORT ) ).thenReturn( jobTrackerPort );
    jobExecutor.loadClusterConfig( objectId, repository, metaStore, null );
    assertEquals( namedCluster, jobExecutor.getNamedCluster() );
    verify( namedCluster ).setHdfsHost( testHost );
    verify( namedCluster ).setHdfsPort( hdfsPort );
    verify( namedCluster ).setJobTrackerHost( jobTrackerHost );
    verify( namedCluster ).setJobTrackerPort( jobTrackerPort );
  }
}
