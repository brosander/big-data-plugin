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

package org.pentaho.big.data.impl.cluster.tests.hdfs;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.impl.cluster.tests.Constants;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.network.ConnectivityTestFactory;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;
import org.pentaho.runtime.test.result.org.pentaho.runtime.test.result.impl.RuntimeTestResultSummaryImpl;
import org.pentaho.runtime.test.test.impl.BaseRuntimeTest;

import java.util.HashSet;

/**
 * Created by bryan on 8/14/15.
 */
public class PingFileSystemEntryPointTest extends BaseRuntimeTest {
  public static final String HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST =
    "_hadoopFileSystemPingFileSystemEntryPointTest";
  public static final String PING_FILE_SYSTEM_ENTRY_POINT_TEST_NAME = "PingFileSystemEntryPointTest.Name";
  private static final Class<?> PKG = PingFileSystemEntryPointTest.class;
  private final MessageGetterFactory messageGetterFactory;
  private final ConnectivityTestFactory connectivityTestFactory;

  public PingFileSystemEntryPointTest( MessageGetterFactory messageGetterFactory,
                                       ConnectivityTestFactory connectivityTestFactory ) {
    super( NamedCluster.class, Constants.HADOOP_FILE_SYSTEM, HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST,
      messageGetterFactory.create( PKG ).getMessage( PING_FILE_SYSTEM_ENTRY_POINT_TEST_NAME ), new HashSet<String>() );
    this.messageGetterFactory = messageGetterFactory;
    this.connectivityTestFactory = connectivityTestFactory;
  }

  @Override public RuntimeTestResultSummary runTest( Object objectUnderTest ) {
    // Safe to cast as our accepts method will only return true for named clusters
    NamedCluster namedCluster = (NamedCluster) objectUnderTest;
    return new RuntimeTestResultSummaryImpl(
      connectivityTestFactory.create( messageGetterFactory, namedCluster.getHdfsHost(), namedCluster.getHdfsPort(),
        true ).runTest() );
  }
}
