package org.pentaho.big.data.impl.shim.tests;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResult;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.BaseClusterTest;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultEntryImpl;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultImpl;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.shim.ConfigurationException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by bryan on 8/14/15.
 */
public class TestShimLoad extends BaseClusterTest {
  public static final String HADOOP_CONFIGURATION_TEST_SHIM_LOAD = "hadoopConfigurationTestShimLoad";
  private static final Class<?> PKG = TestShimLoad.class;

  public TestShimLoad() {
    super( "Hadoop Configuration", HADOOP_CONFIGURATION_TEST_SHIM_LOAD,
      BaseMessages.getString( PKG, "TestShimLoad.Name" ), true, new HashSet<String>() );
  }

  @Override public ClusterTestResult runTest( NamedCluster namedCluster ) {
    List<ClusterTestResultEntry> clusterTestResultEntries = new ArrayList<>();
    try {
      String identifier =
        HadoopConfigurationBootstrap.getHadoopConfigurationProvider().getActiveConfiguration().getIdentifier();
      clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.INFO,
        BaseMessages.getString( PKG, "TestShimLoad.ShimLoaded.Desc", identifier ),
        BaseMessages.getString( PKG, "TestShimLoad.ShimLoaded.Message", identifier ) ) );
    } catch ( ConfigurationException e ) {
      clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
        BaseMessages.getString( PKG, "TestShimLoad.UnableToLoadShim.Desc" ), e.getMessage(), e ) );
    }
    return new ClusterTestResultImpl( this, clusterTestResultEntries );
  }
}
