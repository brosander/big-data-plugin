package org.pentaho.big.data.impl.shim.pig;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.pig.PigService;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PigShim;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by bryan on 7/6/15.
 */
public class PigServiceImpl implements PigService {
  private final NamedCluster namedCluster;
  private final PigShim pigShim;
  private final HadoopShim hadoopShim;

  public PigServiceImpl( NamedCluster namedCluster, PigShim pigShim, HadoopShim hadoopShim ) {
    this.namedCluster = namedCluster;
    this.pigShim = pigShim;
    this.hadoopShim = hadoopShim;
  }

  @Override public boolean isLocalExecutionSupported() {
    return pigShim.isLocalExecutionSupported();
  }

  @Override
  public int[] executeScript( String scriptPath, ExecutionMode executionMode, List<String> parameters,
                              LogChannelInterface logChannelInterface, VariableSpace variableSpace )
    throws Exception {
    Configuration configuration = hadoopShim.createConfiguration();
    if ( executionMode != ExecutionMode.LOCAL ) {
      List<String> configMessages = new ArrayList<String>();
      hadoopShim.configureConnectionInformation( variableSpace.environmentSubstitute( namedCluster.getHdfsHost() ),
        variableSpace.environmentSubstitute( namedCluster.getHdfsPort() ),
        variableSpace.environmentSubstitute( namedCluster.getJobTrackerHost() ),
        variableSpace.environmentSubstitute( namedCluster.getJobTrackerPort() ), configuration,
        configMessages );
      if ( logChannelInterface != null ) {
        for ( String configMessage : configMessages ) {
          logChannelInterface.logBasic( configMessage );
        }
      }
    }
    URL scriptU;
    String scriptFileS = scriptPath;
    scriptFileS = variableSpace.environmentSubstitute( scriptFileS );
    if ( scriptFileS.indexOf( "://" ) == -1 ) {
      File scriptFile = new File( scriptFileS );
      scriptU = scriptFile.toURI().toURL();
    } else {
      scriptU = new URL( scriptFileS );
    }
    String pigScript = pigShim.substituteParameters( scriptU, parameters );
    Properties properties = new Properties();
    pigShim.configure( properties, executionMode == ExecutionMode.LOCAL ? null : configuration );
    return pigShim.executeScript( pigScript, executionMode == ExecutionMode.LOCAL ? PigShim.ExecutionMode.LOCAL :
      PigShim.ExecutionMode.MAPREDUCE, properties );
  }
}
