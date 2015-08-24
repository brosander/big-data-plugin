package org.pentaho.big.data.impl.shim.tests;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetter;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.BaseClusterTest;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultEntryImpl;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.hadoop.NoShimSpecifiedException;
import org.pentaho.hadoop.shim.ConfigurationException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by bryan on 8/14/15.
 */
public class TestShimLoad extends BaseClusterTest {
  public static final String HADOOP_CONFIGURATION_TEST_SHIM_LOAD = "hadoopConfigurationTestShimLoad";
  public static final String TEST_SHIM_LOAD_NAME = "TestShimLoad.Name";
  public static final String TEST_SHIM_LOAD_SHIM_LOADED_DESC = "TestShimLoad.ShimLoaded.Desc";
  public static final String TEST_SHIM_LOAD_SHIM_LOADED_MESSAGE = "TestShimLoad.ShimLoaded.Message";
  public static final String TEST_SHIM_LOAD_NO_SHIM_SPECIFIED_DESC = "TestShimLoad.NoShimSpecified.Desc";
  public static final String TEST_SHIM_LOAD_UNABLE_TO_LOAD_SHIM_DESC = "TestShimLoad.UnableToLoadShim.Desc";
  private static final Class<?> PKG = TestShimLoad.class;
  private final MessageGetter messageGetter;
  private final HadoopConfigurationBootstrap hadoopConfigurationBootstrap;

  public TestShimLoad( MessageGetterFactory messageGetterFactory ) {
    this( messageGetterFactory, HadoopConfigurationBootstrap.getInstance() );
  }

  public TestShimLoad( MessageGetterFactory messageGetterFactory,
                       HadoopConfigurationBootstrap hadoopConfigurationBootstrap ) {
    super( "Hadoop Configuration", HADOOP_CONFIGURATION_TEST_SHIM_LOAD,
      messageGetterFactory.create( PKG ).getMessage( TEST_SHIM_LOAD_NAME ), true, new HashSet<String>() );
    messageGetter = messageGetterFactory.create( PKG );
    this.hadoopConfigurationBootstrap = hadoopConfigurationBootstrap;
  }

  @Override public List<ClusterTestResultEntry> runTest( NamedCluster namedCluster ) {
    List<ClusterTestResultEntry> clusterTestResultEntries = new ArrayList<>();
    try {
      String activeConfigurationId = hadoopConfigurationBootstrap.getActiveConfigurationId();
      hadoopConfigurationBootstrap.getProvider();
      clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.INFO,
        messageGetter.getMessage( TEST_SHIM_LOAD_SHIM_LOADED_DESC, activeConfigurationId ),
        messageGetter.getMessage( TEST_SHIM_LOAD_SHIM_LOADED_MESSAGE, activeConfigurationId ) ) );
    } catch ( NoShimSpecifiedException e ) {
      clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.ERROR,
        messageGetter.getMessage( TEST_SHIM_LOAD_NO_SHIM_SPECIFIED_DESC ), e.getMessage(), e ) );
    } catch ( ConfigurationException e ) {
      clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.ERROR,
        messageGetter.getMessage( TEST_SHIM_LOAD_UNABLE_TO_LOAD_SHIM_DESC ), e.getMessage(), e ) );
    }
    return clusterTestResultEntries;
  }
}
