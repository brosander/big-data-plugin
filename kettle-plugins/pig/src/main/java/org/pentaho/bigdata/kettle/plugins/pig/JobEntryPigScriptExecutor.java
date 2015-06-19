/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
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

package org.pentaho.bigdata.kettle.plugins.pig;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;
import org.pentaho.bigdata.api.configuration.MutableConfigurationNamespace;
import org.pentaho.bigdata.api.configuration.MutableNamedConfiguration;
import org.pentaho.bigdata.api.configuration.NamedConfiguration;
import org.pentaho.bigdata.api.configuration.NamedConfigurationFactory;
import org.pentaho.bigdata.api.configuration.NamedConfigurationLocatorFactory;
import org.pentaho.bigdata.api.pig.PigServiceLocator;
import org.pentaho.bigdata.kettle.plugins.common.BigDataXmlHelper;
import org.pentaho.bigdata.kettle.plugins.common.XmlUtil;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.logging.KettleLogChannelAppender;
import org.pentaho.di.core.logging.Log4jFileAppender;
import org.pentaho.di.core.logging.Log4jKettleLayout;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LogWriter;
import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobListener;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PigShim;
import org.pentaho.hadoop.shim.spi.PigShim.ExecutionMode;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.File;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Job entry that executes a Pig script either on a hadoop cluster or locally.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
@JobEntry( id = "HadoopPigScriptExecutorPlugin", image = "PIG.svg", name = "HadoopPigScriptExecutorPlugin.Name",
    description = "HadoopPigScriptExecutorPlugin.Description",
    categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData",
    i18nPackageName = "org.pentaho.di.job.entries.pig",
    documentationUrl = "http://wiki.pentaho.com/display/EAI/Pig+Script+Executor" )
public class JobEntryPigScriptExecutor extends JobEntryBase implements Cloneable, JobEntryInterface {

  public static final String FS_DEFAULT_NAME = "fs.default.name";
  public static final String MAPRED_JOB_TRACKER = "mapred.job.tracker";
  public static final String HDFS_HOSTNAME = "hdfs_hostname";
  public static final String HDFS_PORT = "hdfs_port";
  public static final String JOBTRACKER_HOSTNAME = "jobtracker_hostname";
  public static final String JOBTRACKER_PORT = "jobtracker_port";
  public static final String CLUSTER_NAME = "cluster_name";
  public static final String SCRIPT_FILE = "script_file";
  public static final String ENABLE_BLOCKING = "enable_blocking";
  public static final String LOCAL_EXECUTION = "local_execution";
  public static final String SCRIPT_PARAMETERS = "script_parameters";
  public static final String PARAMETER = "parameter";
  public static final String HDFS = "hdfs";
  public static final String MAPRED = "mapred";
  private static Class<?> PKG = JobEntryPigScriptExecutor.class; // for i18n purposes, needed by Translator2!!
                                                                 // $NON-NLS-1$
  private PigServiceLocator pigServiceLocator;

  private NamedConfigurationLocatorFactory namedConfigurationLocatorFactory;

  private NamedConfigurationFactory namedConfigurationFactory;

  private NamedConfiguration namedConfiguration;

  /** URL to the pig script to execute */
  protected String m_scriptFile = "";

  /** True if the job entry should block until the script has executed */
  protected boolean m_enableBlocking;

  /** True if the script should execute locally, rather than on a hadoop cluster */
  protected boolean m_localExecution;

  /** Parameters for the script */
  protected HashMap<String, String> m_params = new HashMap<String, String>();

  /**
   * An extended PrintWriter that sends output to Kettle's logging
   *
   * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
   */
  class KettleLoggingPrintWriter extends PrintWriter {
    public KettleLoggingPrintWriter() {
      super( System.out );
    }

    @Override
    public void println( String string ) {
      logBasic( string );
    }

    @Override
    public void println( Object obj ) {
      println( obj.toString() );
    }

    @Override
    public void write( String string ) {
      println( string );
    }

    @Override
    public void print( String string ) {
      println( string );
    }

    @Override
    public void print( Object obj ) {
      print( obj.toString() );
    }

    @Override
    public void close() {
      flush();
    }
  }

  private void loadClusterConfig( IMetaStore metaStore, ObjectId id_jobentry, Repository rep, Node entrynode )
    throws KettleException {
    // attempt to load from named cluster
    String clusterName = null;
    if ( entrynode != null ) {
      clusterName = XMLHandler.getTagValue( entrynode, CLUSTER_NAME); //$NON-NLS-1$
    } else if ( rep != null ) {
      clusterName = rep.getJobEntryAttributeString( id_jobentry, CLUSTER_NAME ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // load from system first, then fall back to copy stored with job (AbstractMeta)
    if ( ! Const.isEmpty( clusterName ) ) {
      namedConfiguration = namedConfigurationLocatorFactory.create( metaStore ).get( clusterName );
    }
    if ( namedConfiguration == null ) {
      String namenodeHost = "localhost";
      String namenodePort = "8020";
      String jobtrackerHost = "localhost";
      String jobtrackerPort = "50030";
      if ( entrynode != null ) {
        // load default values for cluster & legacy fallback
        namenodeHost = XMLHandler.getTagValue( entrynode, HDFS_HOSTNAME); //$NON-NLS-1$
        namenodePort = XMLHandler.getTagValue( entrynode, HDFS_PORT); //$NON-NLS-1$
        jobtrackerHost = XMLHandler.getTagValue( entrynode, JOBTRACKER_HOSTNAME); //$NON-NLS-1$
        jobtrackerPort = XMLHandler.getTagValue( entrynode, JOBTRACKER_PORT); //$NON-NLS-1$
      } else if ( rep != null ) {
        // load default values for cluster & legacy fallback
        namenodeHost = rep.getJobEntryAttributeString( id_jobentry, HDFS_HOSTNAME );
        namenodePort = rep.getJobEntryAttributeString( id_jobentry, HDFS_PORT ); //$NON-NLS-1$
        jobtrackerHost = rep.getJobEntryAttributeString( id_jobentry, JOBTRACKER_HOSTNAME ); //$NON-NLS-1$
        jobtrackerPort = rep.getJobEntryAttributeString( id_jobentry, JOBTRACKER_PORT ); //$NON-NLS-1$
      }
      MutableNamedConfiguration mutableNamedConfiguration = namedConfigurationFactory.create( null );
      MutableConfigurationNamespace hdfsNamespace = mutableNamedConfiguration.createConfigurationNamespace( HDFS );
      String fsDefaultName = "hdfs://" + namenodeHost + ":" + namenodePort;
      hdfsNamespace.setProperty(FS_DEFAULT_NAME, fsDefaultName);
      MutableConfigurationNamespace mapredNamespace = mutableNamedConfiguration.createConfigurationNamespace( MAPRED );
      String jobTracker = jobtrackerHost + ":" + jobtrackerPort;
      mapredNamespace.setProperty(MAPRED_JOB_TRACKER, jobTracker);
      namedConfiguration = mutableNamedConfiguration;
    }
  }

  private void setLegacyValues( BigDataXmlHelper bigDataXmlHelper, NamedConfiguration namedConfiguration ) {
    Pattern fsDefaultNamePattern = Pattern.compile( "hdfs://(.*?):(.*)" );
    Matcher matcher = fsDefaultNamePattern.matcher(
      namedConfiguration.getConfigurationNamespace( HDFS ).getProperty( FS_DEFAULT_NAME, "hdfs://localhost:8020" ) );
    if ( matcher.matches() ) {
      bigDataXmlHelper.addTag( HDFS_HOSTNAME, matcher.group( 1 ) );
      bigDataXmlHelper.addTag( HDFS_PORT, matcher.group( 2 ) );
    }
    String jobTracker =
      namedConfiguration.getConfigurationNamespace( MAPRED ).getProperty( MAPRED_JOB_TRACKER, "localhost:50030" );
    if ( jobTracker.indexOf( ':' ) >= 0) {
      String[] split = jobTracker.split( ":" );
      bigDataXmlHelper.addTag( JOBTRACKER_HOSTNAME, split[0] );
      bigDataXmlHelper.addTag( JOBTRACKER_PORT, split[1] );
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.job.entry.JobEntryBase#getXML()
   */
  public String getXML() {
    StringBuilder retval = new StringBuilder( super.getXML() );
    try {
      BigDataXmlHelper bigDataXmlHelper = new BigDataXmlHelper();
      String namedConfigurationName = namedConfiguration.getName();
      if ( namedConfigurationName == null ) {
        setLegacyValues( bigDataXmlHelper, namedConfiguration );
      } else {
        bigDataXmlHelper.addTag( CLUSTER_NAME, namedConfigurationName );
      }
      bigDataXmlHelper.addTag( SCRIPT_FILE, m_scriptFile );
      bigDataXmlHelper.addTag( ENABLE_BLOCKING, bigDataXmlHelper.boolToString( m_enableBlocking ) );
      bigDataXmlHelper.addTag( LOCAL_EXECUTION, bigDataXmlHelper.boolToString( m_localExecution ) );
      Element scriptParameters = bigDataXmlHelper.addTag( SCRIPT_PARAMETERS );
      if ( m_params != null ) {
        for ( Map.Entry<String, String> paramEntry : m_params.entrySet() ) {
          Element parameter = bigDataXmlHelper.addTag( scriptParameters, PARAMETER );
          bigDataXmlHelper.addTag( parameter, "name", paramEntry.getKey() );
          bigDataXmlHelper.addTag( parameter, "value", paramEntry.getValue() );
        }
      }
      retval.append( bigDataXmlHelper.getString( 4 ) );
    } catch ( Exception e ) {
      log.logError( e.getMessage(), e );
    }

    return retval.toString();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.job.entry.JobEntryInterface#loadXML(org.w3c.dom.Node, java.util.List, java.util.List,
   * org.pentaho.di.repository.Repository)
   */
  @Override
  public void loadXML( Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers,
      Repository repository, IMetaStore metaStore ) throws KettleXMLException {
    super.loadXML( entrynode, databases, slaveServers, repository, metaStore );

    try {
      loadClusterConfig( metaStore, null, rep, entrynode );
    } catch ( KettleException e ) {
      throw new KettleXMLException( e );
    }
    setRepository( repository );

    m_scriptFile = XMLHandler.getTagValue( entrynode, "script_file" );
    m_enableBlocking = XMLHandler.getTagValue( entrynode, "enable_blocking" ).equalsIgnoreCase( "Y" );
    m_localExecution = XMLHandler.getTagValue( entrynode, "local_execution" ).equalsIgnoreCase( "Y" );

    // Script parameters
    m_params = new HashMap<String, String>();
    Node paramList = XMLHandler.getSubNode( entrynode, "script_parameters" );
    if ( paramList != null ) {
      int numParams = XMLHandler.countNodes( paramList, "parameter" );
      for ( int i = 0; i < numParams; i++ ) {
        Node paramNode = XMLHandler.getSubNodeByNr( paramList, "parameter", i );
        String name = XMLHandler.getTagValue( paramNode, "name" );
        String value = XMLHandler.getTagValue( paramNode, "value" );
        m_params.put( name, value );
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.job.entry.JobEntryBase#loadRep(org.pentaho.di.repository.Repository,
   * org.pentaho.di.repository.ObjectId, java.util.List, java.util.List)
   */
  @Override
  public void loadRep( Repository rep, IMetaStore metaStore, ObjectId id_jobentry, List<DatabaseMeta> databases,
      List<SlaveServer> slaveServers ) throws KettleException {
    if ( rep != null ) {
      super.loadRep( rep, metaStore, id_jobentry, databases, slaveServers );

      loadClusterConfig( metaStore, id_jobentry, rep, null );
      setRepository( rep );

      setScriptFilename( rep.getJobEntryAttributeString( id_jobentry, "script_file" ) );
      setEnableBlocking( rep.getJobEntryAttributeBoolean( id_jobentry, "enable_blocking" ) );
      setLocalExecution( rep.getJobEntryAttributeBoolean( id_jobentry, "local_execution" ) );

      // Script parameters
      m_params = new HashMap<String, String>();
      int numParams = rep.countNrJobEntryAttributes( id_jobentry, "param_name" );
      if ( numParams > 0 ) {
        for ( int i = 0; i < numParams; i++ ) {
          String name = rep.getJobEntryAttributeString( id_jobentry, i, "param_name" );
          String value = rep.getJobEntryAttributeString( id_jobentry, i, "param_value" );
          m_params.put( name, value );
        }
      }
    } else {
      throw new KettleException( "Unable to load from a repository. The repository is null." );
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.job.entry.JobEntryBase#saveRep(org.pentaho.di.repository.Repository,
   * org.pentaho.di.repository.ObjectId)
   */
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_job ) throws KettleException {
    if ( rep != null ) {
      super.saveRep( rep, id_job );

      rep.saveJobEntryAttribute( id_job, getObjectId(), CLUSTER_NAME, m_clusterName ); //$NON-NLS-1$
      try {
        if ( !StringUtils.isEmpty( getClusterName() ) &&
            NamedClusterManager.getInstance().contains( getClusterName(), rep.getMetaStore() ) ) {
          // pull config from NamedCluster
          NamedCluster nc = NamedClusterManager.getInstance().read( getClusterName(), rep.getMetaStore() );
          setJobTrackerHostname( nc.getJobTrackerHost() );
          setJobTrackerPort( nc.getJobTrackerPort() );
          setHDFSHostname( nc.getHdfsHost() );
          setHDFSPort( nc.getHdfsPort() );
        }
      } catch ( MetaStoreException e ) {
        logDebug( e.getMessage(), e );
      }
      rep.saveJobEntryAttribute( id_job, getObjectId(), HDFS_HOSTNAME, m_hdfsHostname );
      rep.saveJobEntryAttribute( id_job, getObjectId(), HDFS_PORT, m_hdfsPort );
      rep.saveJobEntryAttribute( id_job, getObjectId(), JOBTRACKER_HOSTNAME, m_jobTrackerHostname );
      rep.saveJobEntryAttribute( id_job, getObjectId(), JOBTRACKER_PORT, m_jobTrackerPort );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "script_file", m_scriptFile );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "enable_blocking", m_enableBlocking );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "local_execution", m_localExecution );

      if ( m_params != null ) {
        int i = 0;
        for ( String name : m_params.keySet() ) {
          String value = m_params.get( name );
          if ( !Const.isEmpty( name ) && !Const.isEmpty( value ) ) {
            rep.saveJobEntryAttribute( id_job, getObjectId(), i, "param_name", name );
            rep.saveJobEntryAttribute( id_job, getObjectId(), i, "param_value", value );
            i++;
          }
        }
      }
    } else {
      throw new KettleException( "Unable to save to a repository. The repository is null." );
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.job.entry.JobEntryBase#evaluates()
   */
  public boolean evaluates() {
    return true;
  }

  /**
   * Get whether the job entry will block until the script finishes
   *
   * @return true if the job entry will block until the script finishes
   */
  public boolean getEnableBlocking() {
    return m_enableBlocking;
  }

  /**
   * Set whether the job will block until the script finishes
   *
   * @param block
   *          true if the job entry is to block until the script finishes
   */
  public void setEnableBlocking( boolean block ) {
    m_enableBlocking = block;
  }

  /**
   * Set whether the script is to be run locally rather than on a hadoop cluster
   *
   * @param l
   *          true if the script is to run locally
   */
  public void setLocalExecution( boolean l ) {
    m_localExecution = l;
  }

  /**
   * Get whether the script is to run locally rather than on a hadoop cluster
   *
   * @return true if the script is to run locally
   */
  public boolean getLocalExecution() {
    return m_localExecution;
  }

  /**
   * Set the URL to the pig script to run
   *
   * @param filename
   *          the URL to the pig script
   */
  public void setScriptFilename( String filename ) {
    m_scriptFile = filename;
  }

  /**
   * Get the URL to the pig script to run
   *
   * @return the URL to the pig script to run
   */
  public String getScriptFilename() {
    return m_scriptFile;
  }

  /**
   * Set the values of parameters to replace in the script
   *
   * @param params
   *          a HashMap mapping parameter names to values
   */
  public void setScriptParameters( HashMap<String, String> params ) {
    m_params = params;
  }

  /**
   * Get the values of parameters to replace in the script
   *
   * @return a HashMap mapping parameter names to values
   */
  public HashMap<String, String> getScriptParameters() {
    return m_params;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.di.job.entry.JobEntryInterface#execute(org.pentaho.di.core.Result, int)
   */
  public Result execute( final Result result, int arg1 ) throws KettleException {

    result.setNrErrors( 0 );

    // Set up an appender that will send all pig log messages to Kettle's log
    // via logBasic().
    KettleLoggingPrintWriter klps = new KettleLoggingPrintWriter();
    WriterAppender pigToKettleAppender = new WriterAppender( new Log4jKettleLayout( true ), klps );

    Logger pigLogger = Logger.getLogger( "org.apache.pig" );
    Level log4jLevel = getLog4jLevel(parentJob.getLogLevel());
    pigLogger.setLevel( log4jLevel );
    Log4jFileAppender appender = null;
    String logFileName = "pdi-" + this.getName(); //$NON-NLS-1$
    LogWriter logWriter = LogWriter.getInstance();
    try {
      appender = LogWriter.createFileAppender( logFileName, true, false );
      logWriter.addAppender( appender );
      log.setLogLevel( parentJob.getLogLevel() );
      if ( pigLogger != null ) {
        pigLogger.addAppender( pigToKettleAppender );
      }
    } catch ( Exception e ) {
      logError( BaseMessages
          .getString( PKG, "JobEntryPigScriptExecutor.FailedToOpenLogFile", logFileName, e.toString() ) ); //$NON-NLS-1$
      logError( Const.getStackTracker( e ) );
    }

    if ( Const.isEmpty( m_scriptFile ) ) {
      throw new KettleException(
          BaseMessages.getString( PKG, "JobEntryPigScriptExecutor.Error.NoPigScriptSpecified" ) );
    }

    try {
      URL scriptU = null;
      String scriptFileS = m_scriptFile;
      scriptFileS = environmentSubstitute( scriptFileS );
      if ( scriptFileS.indexOf( "://" ) == -1 ) {
        File scriptFile = new File( scriptFileS );
        scriptU = scriptFile.toURI().toURL();
      } else {
        scriptU = new URL( scriptFileS );
      }

      HadoopConfiguration active =
          HadoopConfigurationBootstrap.getHadoopConfigurationProvider().getActiveConfiguration();
      HadoopShim hadoopShim = active.getHadoopShim();
      final PigShim pigShim = active.getPigShim();
      // Make sure we can execute locally if desired
      if ( m_localExecution && !pigShim.isLocalExecutionSupported() ) {
        throw new KettleException( BaseMessages.getString( PKG, "JobEntryPigScriptExecutor.Warning.LocalExecution" ) );
      }

      // configure for connection to hadoop
      Configuration conf = hadoopShim.createConfiguration();
      if ( !m_localExecution ) {
        String hdfsHost = environmentSubstitute( m_hdfsHostname );
        String hdfsP = environmentSubstitute( m_hdfsPort );
        String jobTrackerHost = environmentSubstitute( m_jobTrackerHostname );
        String jobTP = environmentSubstitute( m_jobTrackerPort );

        List<String> configMessages = new ArrayList<String>();
        hadoopShim.configureConnectionInformation( hdfsHost, hdfsP, jobTrackerHost, jobTP, conf, configMessages );
        for ( String m : configMessages ) {
          logBasic( m );
        }
      }

      final Properties properties = new Properties();
      pigShim.configure( properties, m_localExecution ? null : conf );

      // transform the map type to list type which can been accepted by ParameterSubstitutionPreprocessor
      List<String> paramList = new ArrayList<String>();
      if ( m_params != null ) {
        for ( Map.Entry<String, String> entry : m_params.entrySet() ) {
          String name = entry.getKey();
          name = environmentSubstitute( name ); // do environment variable substitution
          String value = entry.getValue();
          value = environmentSubstitute( value ); // do environment variable substitution
          paramList.add( name + "=" + value );
        }
      }

      final String pigScript = pigShim.substituteParameters( scriptU, paramList );
      final ExecutionMode execMode = ( m_localExecution ? ExecutionMode.LOCAL : ExecutionMode.MAPREDUCE );

      if ( m_enableBlocking ) {
        int[] executionStatus = pigShim.executeScript( pigScript, execMode, properties );
        logBasic( BaseMessages.getString( PKG, "JobEntryPigScriptExecutor.JobCompletionStatus",
            "" + executionStatus[0], "" + executionStatus[1] ) );

        if ( executionStatus[1] > 0 ) {
          result.setStopped( true );
          result.setNrErrors( executionStatus[1] );
          result.setResult( false );
        }

        removeAppender( appender, pigToKettleAppender );
        if ( appender != null ) {
          ResultFile resultFile =
              new ResultFile( ResultFile.FILE_TYPE_LOG, appender.getFile(), parentJob.getJobname(), getName() );
          result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
        }
      } else {
        final Log4jFileAppender fa = appender;
        final WriterAppender ptk = pigToKettleAppender;
        final Thread runThread = new Thread() {
          public void run() {
            try {
              int[] executionStatus = pigShim.executeScript( pigScript, execMode, properties );
              logBasic( BaseMessages.getString( PKG, "JobEntryPigScriptExecutor.JobCompletionStatus", ""
                  + executionStatus[0], "" + executionStatus[1] ) );
            } catch ( Exception ex ) {
              ex.printStackTrace();
              result.setStopped( true );
              result.setNrErrors( 1 );
              result.setResult( false );
            } finally {
              removeAppender( fa, ptk );
              if ( fa != null ) {
                ResultFile resultFile =
                    new ResultFile( ResultFile.FILE_TYPE_LOG, fa.getFile(), parentJob.getJobname(), getName() );
                result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
              }
            }
          }
        };

        runThread.start();
        parentJob.addJobListener( new JobListener() {

          @Override
          public void jobStarted( Job job ) throws KettleException {
          }

          @Override
          public void jobFinished( Job job ) throws KettleException {
            if ( runThread.isAlive() ) {
              logMinimal( BaseMessages.getString( PKG, "JobEntryPigScriptExecutor.Warning.AsynctaskStillRunning",
                  getName(), job.getJobname() ) );
            }
          }
        } );
      }
    } catch ( Exception ex ) {
      ex.printStackTrace();
      result.setStopped( true );
      result.setNrErrors( 1 );
      result.setResult( false );
      logError( ex.getMessage(), ex );
    }

    return result;
  }

  private Level getLog4jLevel( LogLevel level ) {
    // KettleLogChannelAppender does not exists in Kette core, so we'll use it from kettle5-log4j-plugin.
    Level log4jLevel = KettleLogChannelAppender.LOG_LEVEL_MAP.get( level );
    return log4jLevel != null ? log4jLevel : Level.INFO;
  }

  protected void removeAppender( Log4jFileAppender appender, WriterAppender pigToKettleAppender ) {

    // remove the file appender from kettle logging
    if ( appender != null ) {
      LogWriter.getInstance().removeAppender( appender );
      appender.close();
    }

    Logger pigLogger = Logger.getLogger( "org.apache.pig" );
    if ( pigLogger != null && pigToKettleAppender != null ) {
      pigLogger.removeAppender( pigToKettleAppender );
      pigToKettleAppender.close();
    }
  }
}
