package org.pentaho.big.data.impl.shim.mapreduce;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.vfs2.FileObject;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.mapreduce.MapReduceJobAdvanced;
import org.pentaho.bigdata.api.mapreduce.PentahoMapReduceJobBuilder;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.version.BuildVersion;
import org.pentaho.hadoop.PluginPropertiesUtil;
import org.pentaho.hadoop.mapreduce.InKeyValueOrdinals;
import org.pentaho.hadoop.mapreduce.OutKeyValueOrdinals;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.fs.FileSystem;
import org.pentaho.hadoop.shim.api.fs.Path;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by bryan on 1/8/16.
 */
public class PentahoMapReduceJobBuilderImpl extends MapReduceJobBuilderImpl implements PentahoMapReduceJobBuilder {
  public static final Class<?> PKG = PentahoMapReduceJobBuilderImpl.class;
  public static final String MAPREDUCE_APPLICATION_CLASSPATH = "mapreduce.application.classpath";
  public static final String DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH =
    "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*,$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*";
  public static final String PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE = "pmr.use.distributed.cache";
  public static final String PENTAHO_MAPREDUCE_PROPERTY_PMR_LIBRARIES_ARCHIVE_FILE = "pmr.libraries.archive.file";
  public static final String PENTAHO_MAPREDUCE_PROPERTY_KETTLE_HDFS_INSTALL_DIR = "pmr.kettle.dfs.install.dir";
  public static final String PENTAHO_MAPREDUCE_PROPERTY_KETTLE_INSTALLATION_ID = "pmr.kettle.installation.id";
  public static final String PENTAHO_MAPREDUCE_PROPERTY_ADDITIONAL_PLUGINS = "pmr.kettle.additional.plugins";
  private final HadoopConfiguration hadoopConfiguration;
  private final HadoopShim hadoopShim;
  private final LogChannelInterface log;
  private boolean cleanOutputPath;
  private LogLevel logLevel;
  private String mapperTransformationXml;
  private String mapperInputStep;
  private String mapperOutputStep;
  private String combinerTransformationXml;
  private String combinerInputStep;
  private String combinerOutputStep;
  private String reducerTransformationXml;
  private String reducerInputStep;
  private String reducerOutputStep;

  public PentahoMapReduceJobBuilderImpl( NamedCluster namedCluster,
                                         HadoopConfiguration hadoopConfiguration,
                                         LogChannelInterface log,
                                         VariableSpace variableSpace ) {
    super( namedCluster, hadoopConfiguration.getHadoopShim(), log, variableSpace );
    this.hadoopConfiguration = hadoopConfiguration;
    this.hadoopShim = hadoopConfiguration.getHadoopShim();
    this.log = log;
  }

  @Override public String getHadoopWritableCompatibleClassName( ValueMetaInterface valueMetaInterface ) {
    Class<?> hadoopWritableCompatibleClass = hadoopShim.getHadoopWritableCompatibleClass( valueMetaInterface );
    if ( hadoopWritableCompatibleClass == null ) {
      return null;
    }
    return hadoopWritableCompatibleClass.getCanonicalName();
  }

  @Override public void setLogLevel( LogLevel logLevel ) {
    this.logLevel = logLevel;
  }

  @Override public void setCleanOutputPath( boolean cleanOutputPath ) {
    this.cleanOutputPath = cleanOutputPath;
  }

  @Override public void verifyTransMeta( TransMeta transMeta, String inputStepName, String outputStepName )
    throws KettleException {
    // Verify the input step: see that the key/value fields are present...
    //
    if ( Const.isEmpty( inputStepName ) ) {
      throw new KettleException( "The input step was not specified" );
    }
    StepMeta inputStepMeta = transMeta.findStep( inputStepName );
    if ( inputStepMeta == null ) {
      throw new KettleException( "The input step with name '" + inputStepName + "' could not be found" );
    }

    // Get the fields coming out of the input step...
    //
    RowMetaInterface injectorRowMeta = transMeta.getStepFields( inputStepMeta );

    // Verify that the key and value fields are found
    //
    InKeyValueOrdinals inOrdinals = new InKeyValueOrdinals( injectorRowMeta );
    if ( inOrdinals.getKeyOrdinal() < 0 || inOrdinals.getValueOrdinal() < 0 ) {
      throw new KettleException( "key or value is not defined in input step" );
    }

    // make sure that the input step is enabled (i.e. its outgoing hop
    // hasn't been disabled)
    Trans t = new Trans( transMeta );
    t.prepareExecution( null );
    if ( t.getStepInterface( inputStepName, 0 ) == null ) {
      throw new KettleException( "Input step '" + inputStepName + "' does not seem to be enabled in the "
        + "transformation." );
    }

    // Now verify the output step output of the reducer...
    //
    if ( Const.isEmpty( outputStepName ) ) {
      throw new KettleException( "The output step was not specified" );
    }

    StepMeta outputStepMeta = transMeta.findStep( outputStepName );
    if ( outputStepMeta == null ) {
      throw new KettleException( "The output step with name '" + outputStepName + "' could not be found" );
    }

    // It's a special step designed to map the output key/value pair fields...
    //
    if ( outputStepMeta.getStepMetaInterface().getClass().getCanonicalName()
      .equals( "org.pentaho.big.data.kettle.plugins.mapreduce.step.HadoopExitMeta" ) ) {
      // Get the row fields entering the output step...
      //
      RowMetaInterface outputRowMeta = transMeta.getPrevStepFields( outputStepMeta );
      StepMetaInterface exitMeta = outputStepMeta.getStepMetaInterface();

      List<CheckResultInterface> remarks = new ArrayList<>();
      exitMeta.check( remarks, transMeta, outputStepMeta, outputRowMeta, null, null, null );
      StringBuilder message = new StringBuilder();
      for ( CheckResultInterface remark : remarks ) {
        if ( remark.getType() == CheckResultInterface.TYPE_RESULT_ERROR ) {
          message.append( message.toString() ).append( Const.CR );
        }
      }
      if ( message.length() > 0 ) {
        throw new KettleException( "There was a validation error with the Hadoop Output step:" + Const.CR + message );
      }
    } else {
      // Any other step: verify that the outKey and outValue fields exist...
      //
      RowMetaInterface outputRowMeta = transMeta.getStepFields( outputStepMeta );
      OutKeyValueOrdinals outOrdinals = new OutKeyValueOrdinals( outputRowMeta );
      if ( outOrdinals.getKeyOrdinal() < 0 || outOrdinals.getValueOrdinal() < 0 ) {
        throw new KettleException( "outKey or outValue is not defined in output stream" ); //$NON-NLS-1$
      }
    }
  }

  @Override public void setCombinerInfo( String combinerTransformationXml, String combinerInputStep, String combinerOutputStep ) {
    this.combinerTransformationXml = combinerTransformationXml;
    this.combinerInputStep = combinerInputStep;
    this.combinerOutputStep = combinerOutputStep;
  }

  @Override public void setReducerInfo( String reducerTransformationXml, String reducerInputStep, String reducerOutputStep ) {
    this.reducerTransformationXml = reducerTransformationXml;
    this.reducerInputStep = reducerInputStep;
    this.reducerOutputStep = reducerOutputStep;
  }

  @Override public void setMapperInfo( String mapperTransformationXml, String mapperInputStep, String mapperOutputStep ) {
    this.mapperTransformationXml = mapperTransformationXml;
    this.mapperInputStep = mapperInputStep;
    this.mapperOutputStep = mapperOutputStep;
  }

  @Override protected void configure( Configuration conf ) throws Exception {
    conf.setMapRunnerClass( hadoopShim.getPentahoMapReduceMapRunnerClass() );

    conf.set( "transformation-map-xml", mapperTransformationXml );
    conf.set( "transformation-map-input-stepname", mapperInputStep );
    conf.set( "transformation-map-output-stepname", mapperOutputStep );

    if (combinerTransformationXml != null) {
      conf.set( "transformation-combiner-xml", combinerTransformationXml );
      conf.set( "transformation-combiner-input-stepname", combinerInputStep );
      conf.set( "transformation-combiner-output-stepname", combinerOutputStep );
      conf.setCombinerClass( hadoopShim.getPentahoMapReduceCombinerClass() );
    }
    if ( reducerTransformationXml != null ) {
      conf.set( "transformation-reduce-xml", reducerTransformationXml );
      conf.set( "transformation-reduce-input-stepname", reducerInputStep );
      conf.set( "transformation-reduce-output-stepname", reducerOutputStep );
      conf.setReducerClass( hadoopShim.getPentahoMapReduceReducerClass() );
    }
    conf.setJarByClass( hadoopShim.getPentahoMapReduceMapRunnerClass() );
    conf.setStrings( "logLevel", logLevel.toString() );

    super.configure( conf );
  }

  @Override protected MapReduceJobAdvanced submit( Configuration conf ) throws IOException {
    cleanOutputPath( conf );

    FileSystem fs = hadoopShim.getFileSystem( conf );
    Properties pmrProperties;
    try {
      pmrProperties = loadPMRProperties();
    } catch ( KettleFileException e ) {
      throw new IOException( e );
    }
    // Only configure our job to use the Distributed Cache if the pentaho-mapreduce
    if ( useDistributedCache( conf, pmrProperties ) ) {
      String installPath =
        getProperty( conf, pmrProperties, PENTAHO_MAPREDUCE_PROPERTY_KETTLE_HDFS_INSTALL_DIR, null );
      String installId =
        getProperty( conf, pmrProperties, PENTAHO_MAPREDUCE_PROPERTY_KETTLE_INSTALLATION_ID, BuildVersion
          .getInstance().getVersion() );
      try {
        if ( Const.isEmpty( installPath ) ) {
          throw new IllegalArgumentException( BaseMessages.getString( PKG,
            "JobEntryHadoopTransJobExecutor.KettleHdfsInstallDirMissing" ) );
        }
        if ( Const.isEmpty( installId ) ) {
          String pluginVersion = new PluginPropertiesUtil().getVersion();

          installId = BuildVersion.getInstance().getVersion();
          if ( pluginVersion != null ) {
            installId = installId + "-" + pluginVersion;
          }

          installId = installId + "-" + hadoopConfiguration.getIdentifier();
        }
        if ( !installPath.endsWith( Const.FILE_SEPARATOR ) ) {
          installPath += Const.FILE_SEPARATOR;
        }
        Path kettleEnvInstallDir = fs.asPath( installPath, installId );
        PluginInterface plugin = getPluginInterface();
        FileObject pmrLibArchive =
          KettleVFS.getFileObject( plugin.getPluginDirectory().getPath() + Const.FILE_SEPARATOR
            + getProperty( conf, pmrProperties, PENTAHO_MAPREDUCE_PROPERTY_PMR_LIBRARIES_ARCHIVE_FILE, null ) );
        // Make sure the version we're attempting to use is installed
        if ( hadoopShim.getDistributedCacheUtil().isKettleEnvironmentInstalledAt( fs, kettleEnvInstallDir ) ) {
          log.logDetailed( BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.UsingKettleInstallationFrom",
            kettleEnvInstallDir.toUri().getPath() ) );
        } else {
          // Load additional plugin folders as requested
          String additionalPluginNames =
            getProperty( conf, pmrProperties, PENTAHO_MAPREDUCE_PROPERTY_ADDITIONAL_PLUGINS, null );
          installKettleEnvironment( hadoopShim, pmrLibArchive, fs, kettleEnvInstallDir, additionalPluginNames );
        }
        configureWithKettleEnvironment( hadoopShim, conf, fs, kettleEnvInstallDir );
      } catch ( Exception ex ) {
        throw new IOException(
          BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.InstallationOfKettleFailed" ), ex );
      }
    }

    return super.submit( conf );
  }

  /**
   * Install the Kettle environment, packaged in {@code pmrLibArchive} into the destination within the file systme
   * provided.
   *
   * @param shim              Hadoop Shim to work with
   * @param pmrLibArchive     Archive that contains the libraries required to run Pentaho MapReduce (Kettle's
   *                          dependencies)
   * @param fs                File system to install the Kettle environment into
   * @param destination       Destination path within {@code fs} to install into
   * @param additionalPlugins Any additional plugin directories to copy into the installation
   * @throws KettleException
   * @throws IOException
   */
  public void installKettleEnvironment( HadoopShim shim, FileObject pmrLibArchive, FileSystem fs, Path destination,
                                        String additionalPlugins ) throws Exception {
    if ( pmrLibArchive == null ) {
      throw new KettleException( BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.UnableToLocateArchive",
        pmrLibArchive ) );
    }

    log.logBasic( BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.InstallingKettleAt", destination ) );

    FileObject bigDataPluginFolder = KettleVFS.getFileObject( getPluginInterface().getPluginDirectory().getPath() );
    shim.getDistributedCacheUtil().installKettleEnvironment( pmrLibArchive, fs, destination, bigDataPluginFolder,
      additionalPlugins );

    log.logBasic( BaseMessages
      .getString( PKG, "JobEntryHadoopTransJobExecutor.InstallationOfKettleSuccessful", destination ) );
  }

  /**
   * Configure the provided configuration to use the Distributed Cache backed by the Kettle Environment installed at the
   * installation directory provided.
   *
   * @param shim                Hadoop Shim to work with
   * @param conf                Configuration to update
   * @param fs                  File system that contains the Kettle environment to use
   * @param kettleEnvInstallDir Kettle environment installation path
   * @throws IOException
   * @throws KettleException
   */
  @VisibleForTesting void configureWithKettleEnvironment( HadoopShim shim, Configuration conf, FileSystem fs,
                                                          Path kettleEnvInstallDir ) throws Exception {
    if ( !shim.getDistributedCacheUtil().isKettleEnvironmentInstalledAt( fs, kettleEnvInstallDir ) ) {
      throw new KettleException( BaseMessages.getString( PKG,
        "JobEntryHadoopTransJobExecutor.KettleInstallationMissingFrom", kettleEnvInstallDir.toUri().getPath() ) );
    }

    log.logBasic( BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.ConfiguringJobWithKettleAt",
      kettleEnvInstallDir.toUri().getPath() ) );

    String mapreduceClasspath = conf.get( MAPREDUCE_APPLICATION_CLASSPATH, DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH );
    conf.set( MAPREDUCE_APPLICATION_CLASSPATH, "classes/," + mapreduceClasspath );

    shim.getDistributedCacheUtil().configureWithKettleEnvironment( conf, fs, kettleEnvInstallDir );
    log.logBasic( MAPREDUCE_APPLICATION_CLASSPATH + ": " + conf.get( MAPREDUCE_APPLICATION_CLASSPATH ) );
  }

  /**
   * @return The plugin.properties from the plugin installation directory
   * @throws KettleFileException
   * @throws IOException
   */
  public Properties loadPMRProperties() throws KettleFileException, IOException {
    PluginInterface plugin = getPluginInterface();
    return new PluginPropertiesUtil().loadPluginProperties( plugin );
  }

  /**
   * @return the plugin interface for this job entry.
   */
  public PluginInterface getPluginInterface() {
    return PluginRegistry.getInstance().findPluginWithId( LifecyclePluginType.class, HadoopSpoonPlugin.PLUGIN_ID );
  }

  /**
   * Should the DistributedCache be used for this job execution?
   *
   * @param conf          Configuration to check for the property
   * @param pmrProperties Properties to check for the property
   * @return {@code true} if either {@code conf} or {@code pmrProperties} contains {@code
   * PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE}
   */
  public boolean useDistributedCache( Configuration conf, Properties pmrProperties ) {
    return Boolean.parseBoolean( getProperty( conf, pmrProperties, PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE,
      Boolean.toString( true ) ) );
  }

  /**
   * Gets a property from the configuration. If it is missing it will load it from the properties provided. If it cannot
   * be found there the default value provided will be used.
   *
   * @param conf         Configuration to check for property first.
   * @param properties   Properties to check for property second.
   * @param propertyName Name of the property to return
   * @param defaultValue Default value to use if no property by the given name could be found in {@code conf} or {@code
   *                     properties}
   * @return Value of {@code propertyName}
   */
  public String getProperty( Configuration conf, Properties properties, String propertyName, String defaultValue ) {
    String fromConf = conf.get( propertyName );
    return !Const.isEmpty( fromConf ) ? fromConf : properties.getProperty( propertyName, defaultValue );
  }

  private void cleanOutputPath( Configuration conf ) throws IOException {
    if ( cleanOutputPath ) {
      FileSystem fs = hadoopShim.getFileSystem( conf );
      Path path = getOutputPath( conf, fs );
      String outputPath = path.toUri().toString();
      if ( log.isBasic() ) {
        log.logBasic( BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.CleaningOutputPath", outputPath ) );
      }
      try {
        if ( !fs.exists( path ) ) {
          // If the path does not exist one could think of it as "already cleaned"
          return;
        }
        if ( !fs.delete( path, true ) ) {
          log.logBasic(
            BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.FailedToCleanOutputPath", outputPath ) );
        }
      } catch ( IOException ex ) {
        throw new IOException(
          BaseMessages.getString( PKG, "JobEntryHadoopTransJobExecutor.ErrorCleaningOutputPath", outputPath ), ex );
      }
    }
  }
}
